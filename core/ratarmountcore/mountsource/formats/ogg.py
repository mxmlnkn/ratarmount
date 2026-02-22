import dataclasses
import json
import logging
import os
import stat
import struct
import threading
import time
from pathlib import Path
from typing import IO, Optional, Union

from ratarmountcore.formats import FileFormatID, might_be_format
from ratarmountcore.mountsource.MountSource import FileInfo
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.StenciledFile import StenciledFile
from ratarmountcore.utils import overrides

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class OGGPageHeader:
    version: int
    flags: int
    granule_pos: int
    stream_id: int
    sequence_number: int
    crc32: int
    page_segments: int


def parse_ogg_page_header(data: bytes):
    if data[:4] != b'OggS':
        raise ValueError("Invalid OGG capture pattern!")

    header = OGGPageHeader(*struct.unpack("<BBQIIIB", data[4:27]))
    if header.version != 0:
        raise ValueError("Invalid OGG version!")

    return header


@dataclasses.dataclass
class OGGPage:
    header_offset: int
    data_offset: int
    data_size: int


@dataclasses.dataclass
class OGGStream:
    serial: int
    pages: list = dataclasses.field(default_factory=list)  # (offset, size, granule_pos)
    last_sequence_number: int = -1
    kind: str = 'unknown'
    media_type: str = ''
    codec: str = ''
    subtype: str = ''
    stencils: list[tuple[int, int]] = dataclasses.field(default_factory=list)  # offset, size to expose raw data


def detect_media_type(data: bytes):
    # https://wiki.xiph.org/index.php/MIMETypesCodecs
    # fmt: off
    oggCodecs = [
        ('audio', 'celt'    , b'CELT    '),
        ('audio', 'flac'    , b'\x7FFLAC'),  # Interestingly different from the raw FLAC magic number 'fLaC'
        ('audio', 'opus'    , b'OpusHead'),
        ('audio', 'pcm'     , b'PCM     '),
        ('audio', 'speex'   , b'Speex   '),
        ('audio', 'vorbis'  , b'\x01vorbis'),
        ('audio', 'ogm'     , b'\x01audio'),
        ('text' , 'cmml'    , b'CMML\x00\x00\x00\x00'),
        ('text' , 'kate'    , b'\x80kate\x00\x00\x00'),
        ('text' , 'midi'    , b'OggMIDI\x00'),
        ('text' , 'ogm'     , b'\x01text'),
        ('video', 'dirac'   , b'BBCD\x00'),
        ('video', 'jng'     , b'\213JNG\r\n\032\n'),
        ('video', 'mng'     , b'\212MNG\r\n\032\n'),
        ('video', 'png'     , b'\211PNG\r\n\032\n'),
        ('video', 'theora'  , b'\x80theora'),
        ('video', 'yuv4mpeg', b'YUV4MPEG'),
        ('video', 'ogm'     , b'\x01video'),
    ]
    # fmt: on
    for media_type, codec, magic_bytes in oggCodecs:
        if data.startswith(magic_bytes):
            return media_type, codec
    logger.warning("Unknown stream: %s", data[:100])
    return 'unknown', 'unknown'


@dataclasses.dataclass
class OGMStreamHeaderVideo:
    width: int
    height: int


@dataclasses.dataclass
class OGMStreamHeaderAudio:
    channels: int
    block_align: int
    average_bytes_per_second: int


@dataclasses.dataclass
class OGMStreamHeader:
    """
    https://web.archive.org/web/20050418233257/http://tobias.everwicked.com/packfmt.htm

    typedef struct stream_header_video
    {
        ogg_int32_t width;
        ogg_int32_t height;
    } stream_header_video;

    typedef struct stream_header_audio
    {
        ogg_int16_t channels;
        ogg_int16_t blockalign;
        ogg_int32_t avgbytespersec;
    } stream_header_audio;

    typedef struct stream_header
    {
        char streamtype[8];
        char subtype[4];

        ogg_int32_t size; // size of the structure

        ogg_int64_t time_unit; // in reference time
        ogg_int64_t samples_per_unit;
        ogg_int32_t default_len; // in media time

        ogg_int32_t buffersize;
        ogg_int16_t bits_per_sample;

        union
        {
            // Video specific
            stream_header_video video;
            // Audio specific
            stream_header_audio audio;
        };
    } stream_header;

    -> after bits_per_sample, there is some implied alignment padding of 16 bit!
    -> Isn't the "size of the structure" redundant and always fixed?
    """

    stream_type: bytes
    subtype: bytes

    size: int

    time_unit: int
    samples_per_unit: int
    default_len: int

    buffersize: int
    bits_per_sample: int

    alignment_padding: bytes
    stream_header: bytes
    video_header: Optional[OGMStreamHeaderVideo] = None
    audio_header: Optional[OGMStreamHeaderAudio] = None


# fmt: off
OGM_STREAM_HEADER_STRUCT = struct.Struct(
    "<"   # little endian
    "8s"  # streamtype[8]
    "4s"  # subtype[4]
    "i"   # size
    "q"   # time_unit
    "q"   # samples_per_unit
    "i"   # default_len
    "i"   # buffersize
    "h"   # bits_per_sample
    "h"   # alignment_padding
    "8s"  # union (8 bytes)
)
# fmt: on


def parse_ogg_comment_string(data: bytes, offset: int):
    size = int.from_bytes(data[offset : offset + 4], 'little')
    offset += 4
    if offset + size > len(data):
        raise ValueError(f"Expected string length {size} but only got {len(data)} to read!")
    return 4 + size, data[offset : offset + size].decode('utf-8', errors='replace')


def parse_ogg_comments(data: bytes):
    """
    https://en.wikipedia.org/wiki/Vorbis_comment
    https://www.xiph.org/vorbis/doc/v-comment.html

    > 1. Vendor string length (32 bit unsigned quantity specifying number of octets)
    > 2. Vendor string ([vendor string length] octets coded from beginning of string to end of string,
    >                   not null terminated)
    > 3. Number of comment fields (32 bit unsigned quantity specifying number of fields)
    > 4. Comment field 0 length (if [Number of comment fields]>0; 32 bit unsigned quantity specifying number of octets)
    > 5. Comment field 0 ([Comment field 0 length] octets coded from beginning of string to end of string,
    >                     not null terminated)
    > 6. Comment field 1 length (if [Number of comment fields]>1...)...
    """
    offset = 0
    read_bytes, vendor = parse_ogg_comment_string(data, offset)
    offset += read_bytes

    comment_count = int.from_bytes(data[offset : offset + 4], 'little')
    offset += 4
    comments = {}
    for _ in range(comment_count):
        read_bytes, comment = parse_ogg_comment_string(data, offset)
        offset += read_bytes
        key, value = comment.split('=', 1)
        comments[key] = value

    if offset >= len(data):
        logger.warning("Missing data in packet to finish parsing Vorbis comment!")
    else:
        if data[offset] != 1:
            logger.warning("Missing end of OGG Vorbis comment framing bit!")

        processedSize = offset + 1
        if processedSize < len(data):
            logger.warning("Chunk data in this packet after the parsed Vorbis comment!")

    return vendor, comments


class OGMStreamParser:
    """
    https://www.bunkus.org/videotools/ogmtools/
    https://web.archive.org/web/20050418233257/http://tobias.everwicked.com/packfmt.htm

        First packet (header)
        ---------------------

        pos    | content                 | description
        -------+-------------------------+----------------------------------
        0x0000 | 0x01                    | indicates 'header packet'
        -------+-------------------------+----------------------------------
        0x0001 | stream_header           | the size is indicated in the
               |                         | size member

           -> See OGM_STREAM_HEADER_STRUCT

        Second packet (comment)
        -----------------------

        pos    | content                 | description
        -------+-------------------------+----------------------------------
        0x0000 | 0x03                    | indicates 'comment packet'
        -------+-------------------------+----------------------------------
        0x0001 | data                    | see vorbis doc on www.xiph.org

        -> maybe this was meant? https://xiph.org/vorbis/doc/v-comment.html

        Data packets
        ------------

        pos      | content                 | description
        ---------+-------------------------+----------------------------------
        0x0000   | Bit0  0                 | indicates data packet
                 | Bit1  Bit 2 of lenbytes |
                 | Bit2  unused            |
                 | Bit3  keyframe          |
                 | Bit4  unused            |
                 | Bit5  unused            |
                 | Bit6  Bit 0 of lenbytes |
                 | Bit7  Bit 1 of lenbytes |
        ---------+-------------------------+----------------------------------
        0x0001   | LowByte                 | Length of this packet in samples
                 | ...                     | (frames for video, samples for
                 | HighByte                | audio, 1ms units for text)
        ---------+-------------------------+----------------------------------
        0x0001+  | data                    | packet contents
        lenbytes |                         |

     - In r_ogm.cpp the lenbytes are only used for case OGM_STREAM_TYPE_VIDEO and OGM_STREAM_TYPE_TEXT.
     - This description implies a certain order on the packets, which seems to have been true in my case,
       but in ogminfo.c any order and even multiple header or comment packets are handled. This works
       reliably because, similar to DEFLATE, the first bit encodes whether it is a non-data or data packet
       and the second bit encodes whether the non-data packet is a header or comment packet.

    Subtitles have no real format. The data packet length in 1ms is used for timestamping and the packet contents
    are the raw subtitles in text!
        text, lenbytes: 2 -> duration: 29576, b'\x00'
        text, lenbytes: 2 -> duration: 2127 , b'What, what is it?\r\x00'
        text, lenbytes: 2 -> duration: 3112 , b'\x00'
        text, lenbytes: 2 -> duration: 2059 , b"What's happening? Wow!\r\x00"
        text, lenbytes: 2 -> duration: 50260, b'\x00'
    It may be difficult and wasteful to expose this data via StenciledFile. The stencils would be longer than the
    file itself.
    """

    def __init__(self) -> None:
        self._packetInProgress = bytearray()
        self._packetCount = 0
        self._stream_header: Optional[OGMStreamHeader] = None

    @staticmethod
    def parse_stream_header(data: bytes) -> OGMStreamHeader:
        result = OGMStreamHeader(*OGM_STREAM_HEADER_STRUCT.unpack(data[1:53]))
        if result.stream_type.startswith(b'video'):
            result.video_header = OGMStreamHeaderVideo(*struct.Struct("<ii").unpack(result.stream_header))
        elif result.stream_type.startswith(b'audio'):
            result.audio_header = OGMStreamHeaderAudio(*struct.Struct("<hhi").unpack(result.stream_header))
        return result

    def feed_packet(self, packet: bytes) -> None:
        if self._packetCount == 0:
            self._stream_header = self.parse_stream_header(packet)

        elif self._packetCount == 1:
            comment_magic = b'\x03vorbis'
            if packet.startswith(comment_magic):  # Part of the OGM specification as described below.
                self._comments = parse_ogg_comments(packet[len(comment_magic) :])
            else:
                logger.warning("Not an OGM comment packet but expected one!")

        elif self._packetCount >= 2:
            assert self._stream_header
            if not packet:
                return

            flags = packet[0]
            if flags & 1:  # bit 0 should be 0 for data.
                logger.warning("Invalid OGM data packet flag.")
                return

            duration_bytes = ((flags >> 6) & 3) + (((flags >> 1) & 1) << 2)  # Extract lenbytes
            duration = int.from_bytes(packet[1 : 1 + duration_bytes], 'little')
            if duration_bytes > 0 and self._packetCount < 10 and self._stream_header.stream_type.startswith(b'video'):
                logger.info(
                    "%s, lenbytes: %s -> duration (frames, samples, or ms): %s, %s",
                    self._stream_header.stream_type,
                    duration_bytes,
                    duration,
                    packet[1 + duration_bytes :],
                )

        self._packetCount += 1

    def feed(self, fileObject: IO[bytes], segments: list[int]) -> None:
        packetSize = 0
        # For data packets: 2 fixed bytes with flags and up to 64-bit variable-length value.
        # TODO I am unsure how much is enough. For subtitles, it might be necessary, to read everything.
        #      Currently, support for OGG unwrapping into other formats/raw codecs is on hold anyway.
        maxRequiredDataPacketSize = 2 + 8
        for segment in segments:
            packetSize += segment
            if segment < 255:
                if self._packetCount < 2:
                    self._packetInProgress += fileObject.read(packetSize)
                elif len(self._packetInProgress) < maxRequiredDataPacketSize:
                    byteCount = min(packetSize, maxRequiredDataPacketSize - len(self._packetInProgress))
                    self._packetInProgress += fileObject.read(byteCount)

                self.feed_packet(bytes(self._packetInProgress))
                self._packetInProgress = bytearray()
                packetSize = 0

        if packetSize > 0:
            self._packetInProgress += fileObject.read(packetSize)


def parse_ogg(fileObject: IO[bytes], raw: bool = True) -> dict[int, OGGStream]:
    """
    raw: If true, simply demultiplex the OGG logical streams into separate OGG files.
         If false, unwrap from OGG format into raw format. Unfortunately, this is not possible for many,
         e.g., for subtitles because it is a custom format, which cannot be simply unwrapped via stencils.

    Official OGG documentation:

     - https://www.xiph.org/ogg/doc/rfc3533.txt
     - https://www.xiph.org/ogg/doc/rfc3534.txt
       https://datatracker.ietf.org/doc/html/rfc3534
       updated by: https://datatracker.ietf.org/doc/html/rfc5334
       updated by: https://datatracker.ietf.org/doc/html/rfc7845
     - https://xiph.org/ogg/
     - https://hardwarebug.org/2010/03/03/ogg-objections/
     - https://wiki.xiph.org/index.php/MIMETypesCodecs
    """
    streams: dict[int, OGGStream] = {}
    ogm_streams: dict[int, OGMStreamParser] = {}
    complete_streams: dict[int, OGGStream] = {}

    while True:
        page_offset = fileObject.tell()
        header_bytes = fileObject.read(27)
        if len(header_bytes) < 27:
            if header_bytes:
                logger.warning("Detected chunk data after OGG streams: %s", header_bytes)
            break

        header = parse_ogg_page_header(header_bytes)
        stream_id = header.stream_id

        # The segment table are 8-bit numbers, that's why a simple 'list(bytes)' works.
        # This scheme also encodes "packet" boundary information, which is important for parsing OGM.
        segment_table = list(fileObject.read(header.page_segments))
        data_size = sum(list(segment_table))
        data_offset = fileObject.tell()

        is_beginning_of_stream = header.flags & 0x02 != 0
        if is_beginning_of_stream != (stream_id not in streams):
            raise ValueError("The beginning-of-stream flag must be set exactly for the first page of each stream!")

        if stream_id not in streams:
            # TODO Deriving the packet boundaries from this might be important for OGM parsing
            stream_data = fileObject.read(data_size)
            media_type, subtype = detect_media_type(stream_data)
            # if subtype == 'ogm':
            #    ogm_streams[stream_id] = OGMStreamParser()
            streams[stream_id] = OGGStream(stream_id, media_type=media_type, subtype=subtype)

        if stream_id in ogm_streams:
            fileObject.seek(data_offset)
            ogm_streams[stream_id].feed(fileObject, segment_table)

        fileObject.seek(data_offset + data_size)  # Skip over stream data for now.

        if header.sequence_number <= streams[stream_id].last_sequence_number:
            raise ValueError(
                "The page sequence number must be strictly monotonically increasing! "
                f"Expected {streams[stream_id].last_sequence_number + 1} but got {header.sequence_number}"
            )

        skipped_pages = streams[stream_id].last_sequence_number + 1 - header.sequence_number
        if skipped_pages > 0:
            logger.warning(
                "The page sequence number changed from %s to %s indicating %s missing pages!",
                streams[stream_id].last_sequence_number,
                header.sequence_number,
                skipped_pages,
            )

        # The page size can be derived via data_offset + data_size - page_offset.
        streams[stream_id].pages.append(
            OGGPage(header_offset=page_offset, data_offset=data_offset, data_size=data_size)
        )

        is_end_of_stream = header.flags & 0x04 != 0
        if is_end_of_stream:
            if stream_id in complete_streams:
                logger.warning(
                    "Stream with ID %s appears to have more than one end-of-stream bit flag set! "
                    "This may result in data being ignored.",
                    stream_id,
                )
            complete_streams[stream_id] = streams.pop(stream_id)

    for stream_id, stream in streams.items():
        logger.warning("Stream with ID %s appears incomplete!", stream_id)
        complete_streams[stream_id] = stream

    for stream_id, stream in complete_streams.items():
        logger.debug(
            "Stream %s has %s pages. Type: %s, Codec: %s, Subtype: %s",
            stream_id,
            len(stream.pages),
            stream.media_type,
            stream.codec,
            stream.subtype,
        )

        # For OGG, simply demultiplex the logical OGG streams into separate OGG files
        # because I do not want to implement support for all known codecs.
        # Unfortunately, it is not much easier for OGM. So for now, leave it.
        stream.stencils = [
            (page.header_offset, page.data_offset - page.header_offset + page.data_size) for page in stream.pages
        ]
        # if not raw:
        #    stream.stencils = [(page.data_offset, page.data_size) for page in stream.pages]

    return complete_streams


class OGGMountSource(SQLiteIndexMountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes], Path], **options):
        if isinstance(fileOrPath, Path):
            fileOrPath = str(fileOrPath)
        self.fileObjectLock = threading.Lock()
        self.fileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        if not might_be_format(self.fileObject, FileFormatID.OGG):
            raise ValueError("Not a valid OGG file!")

        self.mtime = os.stat(fileOrPath).st_mtime if isinstance(fileOrPath, str) else time.time()

        indexOptions = {
            'archiveFilePath': fileOrPath if isinstance(fileOrPath, str) else None,
            'backendName': 'PDFMountSource',
        }
        super().__init__(**(options | indexOptions))

        # Extract embedded files and build index
        self._finalize_index(
            lambda: self.index.set_file_infos(
                [self._convert_to_row(stream_id, stream) for stream_id, stream in parse_ogg(self.fileObject).items()]
            )
        )

    def _convert_to_row(self, stream_id: int, stream: OGGStream) -> tuple:
        extension = ''
        extension = '.ogg'
        if stream.subtype == 'ogm':
            extension = '.ogm'
        elif stream.media_type == 'video':
            extension = '.ogv'
        elif stream.media_type == 'audio':
            extension = '.oga'

        name = f"{stream.media_type}_{stream_id:08x}{extension}"
        path, name = SQLiteIndex.normpath(self.transform(name)).rsplit("/", 1)
        mode = stat.S_IFREG | 0o644
        ranges = json.dumps(stream.stencils)
        size = sum(x[1] for x in stream.stencils)
        page = stream.pages[0]  # Use first page's offset to store in the legacy columns.

        # fmt: off
        fileInfo: tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            page.header_offset,  # 2  : header offset
            page.data_offset  ,  # 3  : data offset
            size              ,  # 4  : file size (real decoded file size!)
            self.mtime        ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            ranges            ,  # 8  : linkname, Reuse to store stencil data!
            0                 ,  # 9  : user ID
            0                 ,  # 10 : group ID
            False             ,  # 11 : is TAR (unused?)
            False             ,  # 12 : is sparse
            False             ,  # 13 : is generated (parent folder)
            0                 ,  # 14 : recursion depth
        )
        # fmt: on

        return fileInfo

    @overrides(SQLiteIndexMountSource)
    def open(self, fileInfo: FileInfo, buffering: int = -1) -> IO[bytes]:
        stencils = [(self.fileObject, offset, size) for offset, size in json.loads(fileInfo.linkname)]
        return StenciledFile(fileStencils=stencils, fileObjectLock=self.fileObjectLock)

    @overrides(SQLiteIndexMountSource)
    def close(self) -> None:
        super().close()
        if fobj := getattr(self, 'fileObject', None):
            fobj.close()
