import contextlib
import hashlib
import io
import logging
import os
import stat
import time
from collections.abc import Generator
from pathlib import Path
from typing import IO, Any, Optional, Union

from ratarmountcore.formats import FileFormatID, might_be_format
from ratarmountcore.mountsource.MountSource import FileInfo
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import RatarmountError, overrides

logger = logging.getLogger(__name__)

try:
    import pypdf
except ImportError:
    pypdf = None  # type: ignore


def gather_embedded_files(reader) -> Generator[tuple[str, Any], None, None]:
    # Methods:
    #   cache_get_indirect_object(
    #   cache_indirect_object(
    #   decode_permissions(
    #   decrypt(
    #   page_layout
    #   page_mode
    #   pages
    #   pdf_header  # Returns e.g. '%PDF-1.5'. Might be interesting, but probably not worth to expose as a file.
    #   read(
    #   read_next_end_line(
    #   read_object_header(
    #   resolved_objects
    #   stream
    #       -
    #   get_form_text_fields()
    #   get_object(
    #   get_page_number(
    #   is_encrypted
    #       - [ ] test password-encrypted PDF support. See also 'decrypt'
    #   metadata
    #       -> show as METADATA.json file if not empty
    #   named_destinations -> basically the TOC. Probably not of interest.
    #   outline
    #   get_destination_page_number(
    #   get_fields(
    #   strict
    #   threads
    #   trailer -> maybe something for metadata? Probably uninteresting
    #   xfa
    #   xmp_metadata
    #       -> show as XMP_METADATA.json file if not empty? Cannot test it.
    #   xref
    #   xref_free_entry
    #   xref_index
    #   xref_objStm

    # https://pypdf.readthedocs.io/en/stable/dev/pdf-format.html
    #   PdfReader.xref returns:
    #   {
    #       # Generation (Revision or Version) Number: Objects
    #       0: {
    #           # Object ID: Byte offset in file
    #           1: 15,
    #           2: 54,
    #           3: 113,
    #           ...
    #       },
    #       # Special generation number for free objects.
    #       65535: {0: 0}}

    # https://pypdf2.readthedocs.io/en/3.0.0/user/reading-pdf-annotations.html#attachments
    #  -> This only finds attachments referenced on any page.
    #     A simple counter-example can be created with 3 lines using pypdf itself.
    #     The LaTeX example attaches files without listing them in '/EmbeddedFiles' though.
    #     So we need to implement both :(
    #  -> Instead use: Trailer -> /Root -> /Names -> /EmbeddedFiles -> /Names
    found_objects: list[tuple[str, Any]] = []
    with contextlib.suppress(KeyError):
        files = reader.trailer['/Root']['/Names']['/EmbeddedFiles']['/Names']
        for name, indirect_object in zip(files[0::2], files[1::2]):
            # get_object: {'/Type': '/Filespec', '/F': 'test.bin', '/EF': {'/F': IndirectObject(5, 0, 12345678)}}
            # The name is also stored in indirect_object.get_object()['/F'].
            # stream is PyPDF2.generic._data_structures.DecodedStreamObject
            # /EF = EmbeddedFile, I assume.
            found_objects.append((name, indirect_object.get_object()['/EF']['/F'].indirect_reference))

    # LaTeX attaches files but does not store them under trailer['/Root']['/Names']['/EmbeddedFiles']!
    # Iterating over xref does not seem to find attachments. It only finds the raw data objects and compressed
    # streams without any metadata information such as file/image/font name attached to them.
    for page_number, page in enumerate(reader.pages):
        for reference in page.get('/Annots', []):
            annotation = reference.get_object()
            # TODO: Support other attachments? E.g., /Sound, /Movie, /3D.
            if annotation.get('/Subtype', '') == '/FileAttachment' and '/FS' in annotation:
                annotation = annotation['/FS']
                found_objects.append((annotation['/F'], annotation['/EF']['/F'].indirect_reference))

        # The image names such as Im1.png seem to be auto-generated and therefore of questionable use.
        for image_number, image in enumerate(page.images):
            # TODO do better than this? Heck, maybe simply store a string in the INT column.
            #      It is not checked anyway and also not written out because of other issues.
            offset = -((image_number + 1) * len(reader.pages) + page_number)
            found_objects.append((f'page_{page_number}_{image.name}', offset))

        if page.extract_text():
            found_objects.append((f"page_{page_number}.txt", -page_number))

        # Seems to only have limited use because, in my test case, it returned six files starting with
        # %!PS-AdobeFont-1.0, which is quite obscure, but fontforge can open it, but it contains only the
        # actually used types, i.e., it is not a full font set!
        # with contextlib.suppress(KeyError):
        #    for _, font in page['/Resources']['/Font'].items():
        #        if '/FontDescriptor' not in font:
        #            continue
        #
        #        found_objects.append(
        #            (font['/FontDescriptor']['/FontName'], font['/FontDescriptor']['/FontFile'].indirect_reference)
        #        )

    offsets = set()
    for name, reference in found_objects:
        if isinstance(reference, int):
            if offset not in offsets:
                offsets.add(offset)
                yield name, reference
        else:
            # stream.indirect_reference is different from indirect_object!
            offset = reader.xref[reference.generation][reference.idnum]
            if offset not in offsets:
                offsets.add(offset)
                yield name, offset


class PDFMountSource(SQLiteIndexMountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes], Path], **options):
        if pypdf is None:
            raise ImportError("Failed to find pypdf. Try: pip install pypdf")

        if isinstance(fileOrPath, Path):
            fileOrPath = str(fileOrPath)
        self.fileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        if not might_be_format(self.fileObject, FileFormatID.PDF):
            raise ValueError("Not a valid PDF file!")

        self.mtime = os.stat(fileOrPath).st_mtime if isinstance(fileOrPath, str) else time.time()

        # Force indexes in memory because images sizes are probably not reproducible as
        # they are compressed to PNG on demand.
        indexOptions = {
            'indexFilePath': ':memory:',
            'archiveFilePath': fileOrPath if isinstance(fileOrPath, str) else None,
            'backendName': 'PDFMountSource',
        }
        super().__init__(**(options | indexOptions))

        # Extract embedded files and build index
        self._pdf_reader = pypdf.PdfReader(self.fileObject)
        self._objects_by_offset = {
            offset: (object_id, generation)
            for generation, objects in self._pdf_reader.xref.items()
            for object_id, offset in objects.items()
        }
        self._finalize_index(
            lambda: self.index.set_file_infos(
                [self._convert_to_row(offset, name) for name, offset in gather_embedded_files(self._pdf_reader)]
            )
        )

    def _open_with_offset(self, offset: int) -> Optional[bytes]:
        if offset <= 0:  # The offset cannot be 0 for real because the magic bytes are there.
            page_count = len(self._pdf_reader.pages)
            page_number = -offset % page_count
            page = self._pdf_reader.pages[page_number]
            image_number = -offset // page_count
            return page.extract_text().encode() if image_number == 0 else page.images[image_number - 1].data

        if (
            pdf_object := pypdf.generic.IndirectObject(*self._objects_by_offset[offset], self._pdf_reader).get_object()
        ) and hasattr(pdf_object, 'get_data'):
            return pdf_object.get_data()
        return None

    def _convert_to_row(self, offset: int, name: str) -> tuple:
        data = self._open_with_offset(offset)
        if not name and data:
            name = hashlib.sha256(data).hexdigest()

        path, name = SQLiteIndex.normpath(self.transform(name)).rsplit("/", 1)
        mode = stat.S_IFREG | 0o644
        size = len(data) if data else 0

        # fmt: off
        fileInfo: tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            offset            ,  # 2  : header offset
            0                 ,  # 3  : data offset
            size              ,  # 4  : file size (real decoded file size!)
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

    @overrides(SQLiteIndexMountSource)
    def open(self, fileInfo: FileInfo, buffering: int = -1) -> IO[bytes]:
        userdata = SQLiteIndex.get_index_userdata(fileInfo.userdata)
        data = self._open_with_offset(userdata.offsetheader)
        if not data:
            raise RatarmountError(f"Failed to find PDF object stream at offset {userdata.offsetheader}.")
        return io.BytesIO(data)

    @overrides(SQLiteIndexMountSource)
    def close(self) -> None:
        super().close()
        if fobj := getattr(self, 'fileObject', None):
            fobj.close()
