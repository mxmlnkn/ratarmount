import base64
import encodings
import hashlib
import html
import io
import logging
import mimetypes
import os
import re
import stat
import tarfile
import threading
import time
import urllib.parse
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import IO, Optional, Union

from ratarmountcore.formats import is_html_file
from ratarmountcore.mountsource.MountSource import FileInfo
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import overrides

logger = logging.getLogger(__name__)

# Elements that might contain data URLs found with the following command applied on thousands of stored HTMLs:
# sed 's|<|\n<|g' | sed -nE 's|<([A-Za-z0-9]+)[^>]* ([A-Za-z0-9]+)="data:.*|\1 \2|gp' | sort -u
#     audio     src
#     body      background
#     div       poster
#     div       src
#     embed     src
#     image     href
#     img       src
#     input     src
#     link      href
#     meta      content
#     object    data
#     script    src
#     source    src
#     track     src
#     video     poster
#     video     src
# -> This means that for the 'video' tag, there likely will be multiple attributes!
#    This complicates the algorithm a bit because we only get the offset of the tag, not for each attribute.
# -> In general, simply check all tag values for the prefix: data:image/x-icon;base64,
# 'grep' -r -h -o '"data:[^;]*;base64,' <files> | sort -u
# These are some unexpected outliers:
#   "data:text/html,<script>onresize=function(){parent.postMessage(0,'*')}<\/script>"
#   "data:text/vtt;charset=utf-8,
#   "data:text/javascript;charset=utf-8,
#   "data:image/svg+xml;base64,
#   "data:image\/gif;base64,
#   "data:image\/svg+xml;base64,
#       -> bug? Or is backslash really allowed here?! It is not the only instance!
#   "data:image\u002Fpng;base64,
#   "data:image/svg+xml;charset%3DUS-ASCII,
#   "data:image/svg+xml;charset%3Dutf8,
#       -> URL-encoding is probably legal
#   "data:image/svg+xml;charset=3Dutf-8,
#       -> This must be a bug... It should be %3D or =  but not =3D
#       -> This stems from an mhtml (MIME / EML) file, not HTML! Confusion solved.
#   "data:image/svg+xml;charset=US-ASCII,
#   "data:image/svg+xml;charset=utf-8,
#   "data:image/svg+xml;charset=utf8,
#   "data:image/svg+xml;charset=UTF-8,
#   "data:image/svg+xml;charset=utf-8;base64,
#   "data:image/svg+xml;utf8,
#       -> This looks non-standard! RFC 2397 specifies [";base64"] at the end and all parameters must include "=".
#   "data:image/svg+xml; utf8,
#       -> This looks even more non-standard!
#   "data:image/svg+xml;utf9,
#       -> This must be a bug... https://github.com/enricobacis/utf9
#   "data:image/x-icon;,
#   "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 20 19'>...
#   "data:image/svg+xml,<svg height=%2710px%27 width=%2710px%27 viewBox=%270 0 16 16%27 fill=%27%23000000%27
#   "data:image/svg+xml;nitro-empty-id=MjQzNTo1NzY=-1;base64,
#   "data: image/png;base64,
#   "data:binary/octet-stream;base64,
#  -> The ; seems to be optional, the comma not so.
#  -> It seems like spaces are allowed.
#  -> It seems like arbitrary metadata ('nito-empty-id' by wordpress lazy-loading addon?) is allowed
#  -> The UTF-8 encoding is sometimes even used if there are special characters, which are then URL-encoded.
# https://www.w3schools.com/tags/tag_picture.asp
#  -> some more obscure tags that might contain data URLs not found in my test data:
#       a -> href
#       style -> src
#       iframe -> src
#       source -> src, srcset
#       picture -> srcset
#       track -> src
# https://wiki.selfhtml.org/wiki/Data-URL
# https://stackoverflow.com/questions/273354/are-single-quotes-allowed-in-html


@dataclass
class LineOffset:
    line: int = 0
    offset: int = 0


@dataclass
class EmbeddedFileCandidate:
    # line number (starting at 1) and character offset
    original_url: str
    attribute: str
    start: LineOffset
    next: Optional[LineOffset] = None


# https://datatracker.ietf.org/doc/html/rfc2397
#   data:[<mediatype>][;base64],<data>
#   Syntax
#       dataurl    := "data:" [ mediatype ] [ ";base64" ] "," data
#       mediatype  := [ type "/" subtype ] *( ";" parameter )
#       data       := *urlchar
#       parameter  := attribute "=" value
DATA_URL_MIME_TYPE = """(?P<mime_type>[^;,"']+/[^;,"']+)"""
DATA_URL_PARAMETERS = """(?P<parameters>(;[^;,"']*)*)"""
DATA_URL_PREFIX = f"(?P<data_url>data:{DATA_URL_MIME_TYPE}?{DATA_URL_PARAMETERS},"
DATA_URL_REGEX = re.compile(DATA_URL_PREFIX + ")")
DATA_URL_IN_CSS = re.compile(
    "(/[*]savepage-url=(?P<original_url>[^*]+)[*]/)?url[(]" + DATA_URL_PREFIX + "(?P<data>[^)]+))[)]"
)
DATA_URL_SINGLE_QUOTE = re.compile("'" + DATA_URL_PREFIX + "(?P<data>[^']+))'")
DATA_URL_DOUBLE_QUOTE = re.compile('"' + DATA_URL_PREFIX + '(?P<data>[^"]+))"')


class HTMLDataURLParser(HTMLParser):
    """Parser to extract data URLs from HTML elements."""

    def __init__(self) -> None:
        # handle_data() might be called more than once if convert_charrefs is false.
        # However, it might be necessary to gather all char refs to correctly compute offsets to data URLs.
        # TODO It's a nightmare. For now, let's ignore char/ent refs.
        # https://en.wikipedia.org/wiki/Standard_Generalized_Markup_Language#Document_validity
        # https://en.wikipedia.org/wiki/XML#Escaping
        super().__init__(convert_charrefs=True)
        self.files: list[EmbeddedFileCandidate] = []
        self._char_refs: list[LineOffset] = []
        self._entity_refs: list[LineOffset] = []

    @overrides(HTMLParser)
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str]]) -> None:
        """
        For instance, for the tag <A HREF="https://www.cwi.nl/">, this method would be called as
        handle_starttag('a', [('href', 'https://www.cwi.nl/')]).
        """
        self._update_end_offset()

        # getpos gives the start offset of the current tag.
        if not any(v.startswith('data:') for _, v in attrs):
            return

        attributes = dict(attrs)
        for attribute, value in attributes.items():
            if not value.startswith('data:'):
                continue

            # We are only interested in non-zero length payloads, and the comma is required by RFC 2397.
            comma = value.find(',')
            if comma < 0 or comma + 1 >= len(value):
                continue

            self.files.append(
                EmbeddedFileCandidate(
                    original_url=attributes.get("data-savepage-" + attribute, ""),
                    attribute=attribute,
                    start=LineOffset(*self.getpos()),
                )
            )

    # Overwrite all other handles so that we can extract a guess for the end position corresponding to the
    # getpos result in handle_starttag.

    @overrides(HTMLParser)
    def handle_endtag(self, tag: str) -> None:
        """
        Note that there are start tags without end tags, e.g., <br>. They will trigger this method.
        But tags with such as <iframe .../> will trigger the handle_starttag and handle_endtag methods.
        """
        self._update_end_offset()

    @overrides(HTMLParser)
    def handle_data(self, data: str) -> None:
        self._update_end_offset()

        # Search in data because sometimes it may contain further data URIs, e.g., in CSS style sheets such as:
        # /*savepage-url=/assets/fonts/robotocondensed-regular-webfont.ttf*/url(data:application/x-font-ttf;base64,...
        line, offset = self.getpos()

        self.files.extend(
            EmbeddedFileCandidate(
                original_url=match.group('original_url') or '',
                attribute='',
                start=LineOffset(line, offset + match.span('data_url')[0]),
                next=LineOffset(line, offset + match.span('data_url')[1]),
            )
            for matches in (
                DATA_URL_IN_CSS.finditer(data),
                DATA_URL_SINGLE_QUOTE.finditer(data),
                DATA_URL_DOUBLE_QUOTE.finditer(data),
            )
            for match in matches
        )

    @overrides(HTMLParser)
    def handle_decl(self, data: str) -> None:
        self._update_end_offset()

    @overrides(HTMLParser)
    def handle_entityref(self, name: str) -> None:
        self._entity_refs.append(LineOffset(*self.getpos()))

    @overrides(HTMLParser)
    def handle_charref(self, name: str) -> None:
        self._char_refs.append(LineOffset(*self.getpos()))

    def _update_end_offset(self) -> None:
        if not self.files:
            return

        # getpos can sometimes be equal to the 'start', e.g., for <iframe/> because it seems that handle_starttag
        # and handle_endtag get triggered from the same position.
        file = self.files[-1]
        if file.next is not None or LineOffset(*self.getpos()) == file.start:
            return

        self.files[-1] = EmbeddedFileCandidate(
            original_url=file.original_url,
            attribute=file.attribute,
            start=file.start,
            next=LineOffset(*self.getpos()),
        )


class DataURLFile(io.BytesIO):
    """
    Exposes a file-like interface for data URLs according to RFC2397, e.g.,
        data:text/plain;base64,VGVzdCBQYWdlIHdpdGggZW1iZWQu
    """

    # TODO Implement it in a streaming manner... Might be impossible or require too much memory overhead because
    #      of all the necessary conversions:
    #          bytes -> UTF-8 -> char/entity references -> URL unquoting -> base64 decoding
    def __init__(self, data_url: Optional[str] = None):
        # https://datatracker.ietf.org/doc/html/rfc2397
        # > If <mediatype> is omitted, it defaults to text/plain;charset=US-ASCII.
        # > As a shorthand, "text/plain" can be omitted but the charset parameter supplied.
        self.mime_type = 'text/plain'
        self.valid = False
        self.encoding = 'ascii'  # https://docs.python.org/3/library/codecs.html#standard-encodings
        self.is_base64 = False

        data = urllib.parse.unquote(html.unescape(data_url or ""))
        match = DATA_URL_REGEX.match(data)
        if not match:
            super().__init__()
            return

        if mime_type := match.group('mime_type'):
            self.mime_type = mime_type

        if parameters := match.group('parameters'):
            if parameters.endswith(';base64'):
                self.is_base64 = True

            for parameter in parameters.split(';'):
                parameter = parameter.strip().lower()
                if parameter in {'utf-8', 'utf8'}:
                    self.encoding = 'utf8'
                    continue

                if '=' not in parameter:
                    continue

                key, value = parameter.split('=', 1)
                if key.strip() != 'charset':
                    continue

                if value.strip() == 'us-ascii':
                    self.encoding = 'ascii'
                if standard_encoding := encodings.search_function(value.strip()):
                    self.encoding = standard_encoding.name

        data = data[match.end() :]
        decoded = base64.b64decode(data) if self.is_base64 else data.encode(self.encoding)
        super().__init__(decoded)


# > A name consists of a letter followed by letters, digits, periods, or
# > hyphens. The length of a name is limited to 72 characters
HTML_NAME_PATTERN = "[A-Za-z][A-Za-z0-9.-]*"

# 3.2.4. Attributes
#
# In a start-tag, white space and attributes are allowed between the
# element name and the closing delimiter. An attribute specification
# typically consists of an attribute name, an equal sign, and a value,
# though some attribute specifications may be just a name token. White
# space is allowed around the equal sign.
#
# The value of the attribute may be either:
#
#      * A string literal, delimited by single quotes or double
#      quotes and not containing any occurrences of the delimiting
#      character.
#
#      * A name token (a sequence of letters, digits, periods, or
#      hyphens). Name tokens are not case sensitive.
#
#          NOTE - Some historical implementations allow any
#          character except space or `>' in a name token.
HTML_ATTRIBUTE_VALUE_PATTERN = """(?P<value>'[^']*'|"[^"]*"|[A-Za-z0-9.-]*)"""
HTML_ATTRIBUTE_PATTERN = "(?P<attribute>" + HTML_NAME_PATTERN + r")(\s*=\s*" + HTML_ATTRIBUTE_VALUE_PATTERN + ")?"
HTML_ATTRIBUTE_REGEX = re.compile(fr"\s+{HTML_ATTRIBUTE_PATTERN}")
HTML_START_TAG_REGEX = re.compile(f"<{HTML_NAME_PATTERN}")


def _find_tag_attribute_spans(data: str, attribute: str) -> tuple[int, int]:
    """
    Given 'data', which is assumed to start with an HTML start tag, i.e., '<', not followed by '/',
    find offsets and lengths of the values for the given 'attributes' in this tag.
    """
    # Skip over the name assumed to be at the start.
    # https://www.ietf.org/rfc/rfc1866.txt
    # > A name consists of a letter followed by letters, digits, periods, or
    # > hyphens. The length of a name is limited to 72 characters
    startTag = HTML_START_TAG_REGEX.match(data)
    if not startTag:
        return 0, 0

    position = startTag.end()
    while match := HTML_ATTRIBUTE_REGEX.match(data, position):
        position = match.end()
        if match.group('attribute') == attribute:
            result = match.span('value')
            return (result[0] + 1, result[1] - 1) if data[result[0]] in ["'", '"'] else result

    return 0, 0


@dataclass
class EmbeddedFile:
    # line number (starting at 1) and character offset
    original_url: str
    span: tuple[int, int]


def gather_embedded_files(fileobj: IO[str]) -> list[EmbeddedFile]:
    # In order to convert (line number, row) into a file offset we need to know the character offsets
    # for each line, or even better, the byte offsets for each line! In the worst case, the whole HTML
    # will be on a single line!
    lineOffsets: list[int] = [0]  # list index -> character offset

    parser = HTMLDataURLParser()
    processed_data_size = 0
    while data := fileobj.read(128 * 1024):
        position = 0
        while (position := data.find('\n', position)) >= 0:
            position += 1  # Skip over newline to point to the line start not line end and to progress the loop.
            lineOffsets.append(processed_data_size + position)
        processed_data_size += len(data)

        parser.feed(data)
    file_size = processed_data_size

    def to_char_offset(line_offset: LineOffset):
        return lineOffsets[line_offset.line - 1] + line_offset.offset

    # The TextIOWrapper, used in 'open' with text mode, is kinda insane. The arguments to 'read' are in characters.
    # However, the values to 'seek' and from 'tell' seem to be in bytes! html.parser.getpos seems to really return
    # character offsets. I.e., in order to seek to positions and keep multi-byte UTF-8 support, we need to convert
    # all character positions to byte positions.
    # Avoid any seeks at all by sorting all offsets to convert and creating a dictionary.
    char_to_byte_offset = {}
    fileobj.seek(0)
    processed_data_size = 0
    for offset in sorted(
        [
            o
            for file in parser.files
            for o in [to_char_offset(file.start), to_char_offset(file.next) if file.next else file_size]
        ]
    ):
        assert offset >= processed_data_size
        if offset > processed_data_size:
            data = fileobj.read(offset - processed_data_size)
            processed_data_size += len(data)

        assert offset == processed_data_size
        if offset == processed_data_size:
            char_to_byte_offset[offset] = fileobj.tell()

    files: list[EmbeddedFile] = []
    for file in parser.files:
        start = char_to_byte_offset[to_char_offset(file.start)]
        end = char_to_byte_offset[to_char_offset(file.next) if file.next else file_size]
        if not file.attribute:
            files.append(EmbeddedFile(original_url=file.original_url, span=(start, end)))
            continue

        fileobj.seek(start)
        data = fileobj.read(end - start)
        span = _find_tag_attribute_spans(data, file.attribute)
        if span[1] <= span[0]:
            continue

        # Also convert the adjustments from character offsets to byte offsets.
        # This also has the side effecft that it converts them from relative offsets based on 'start'
        # to absolute offsets.
        fileobj.seek(start)
        fileobj.read(span[0])
        bytes_start = fileobj.tell()
        fileobj.read(span[1] - span[0])
        bytes_end = fileobj.tell()

        files.append(EmbeddedFile(original_url=file.original_url, span=(bytes_start, bytes_end)))

    return files


class HTMLMountSource(SQLiteIndexMountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], encoding: str = tarfile.ENCODING, **options):
        self.mtime = os.stat(fileOrPath).st_mtime if isinstance(fileOrPath, str) else time.time()

        # html.parser seems to be very lenient. Therefore, check manually and hope that the check is lenient enough.
        with open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath as file:
            if not is_html_file(file):
                raise ValueError("Not a valid HTML file!")

        self.fileObjectLock = threading.Lock()
        self.fileObject = (
            open(fileOrPath, encoding=encoding)  # open in text mode!
            if isinstance(fileOrPath, str)
            else io.TextIOWrapper(fileOrPath, encoding=encoding)
        )
        files = gather_embedded_files(self.fileObject)

        indexOptions = {
            'archiveFilePath': fileOrPath if isinstance(fileOrPath, str) else None,
            'backendName': 'HTMLMountSource',
            'encoding': encoding,
        }
        super().__init__(**(options | indexOptions))
        self._finalize_index(lambda: self.index.set_file_infos([self._convert_to_row(file) for file in files]))

    def _convert_to_row(self, file: EmbeddedFile):
        url_file = self._open_with_span(file.span[0], file.span[1])
        contents = url_file.read()

        virtual_path = file.original_url
        # https://github.com/python/cpython/issues/97646
        # There seems to be some issue with application/javascript and text/javascript.
        # The latter returns .js for Python 3.12+ but .es for prior versions.
        if url_file.mime_type.endswith('/javascript'):
            extension = '.js'
        else:
            extension = mimetypes.guess_extension(url_file.mime_type) or ""
        if not virtual_path or virtual_path.startswith('data:'):
            virtual_path = hashlib.sha256(contents).hexdigest() + extension
        if os.path.splitext(virtual_path)[1].lower() != extension.lower():
            virtual_path += extension

        path, name = SQLiteIndex.normpath(self.transform(virtual_path)).rsplit("/", 1)
        mode = stat.S_IFREG | 0o777

        # fmt: off
        fileInfo : tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            file.span[0]      ,  # 2  : header offset (reused in combination with data offset to store the span)
            file.span[1]      ,  # 3  : data offset
            len(contents)     ,  # 4  : file size (real decoded file size!)
            self.mtime        ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            ""                ,  # 8  : linkname
            0                 ,  # 9  : user ID
            0                 ,  # 10 : group ID
            False             ,  # 11 : is TAR (unused?)
            False             ,  # 12 : is sparse
            False             ,  # 13 : is generated (parent folder)
            0                 ,  # 14 : recursion depth
        )
        # fmt: on

        return fileInfo

    def _open_with_span(self, start: int, end: int) -> DataURLFile:
        if end <= start:
            return DataURLFile()

        with self.fileObjectLock:
            self.fileObject.seek(start)
            return DataURLFile(self.fileObject.read(end - start))

    @overrides(SQLiteIndexMountSource)
    def open(self, fileInfo: FileInfo, buffering: int = -1) -> IO[bytes]:
        userdata = SQLiteIndex.get_index_userdata(fileInfo.userdata)
        return self._open_with_span(userdata.offsetheader, userdata.offset)

    @overrides(SQLiteIndexMountSource)
    def close(self) -> None:
        super().close()
        if lock := getattr(self, 'fileObjectLock', None):
            with lock:
                if fobj := getattr(self, 'fileObject', None):
                    fobj.close()
