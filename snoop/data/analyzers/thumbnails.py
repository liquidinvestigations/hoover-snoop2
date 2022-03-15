"""Task that is calling a thumbnail generation service.

Three Thumnbails in different sizes are created. The service used can be found here:
[[https://github.com/FPurchess/preview-service]].
"""

import logging

import requests
from django.conf import settings

from .. import models
from .. import utils
from ..tasks import SnoopTaskBroken, returns_json_blob, snoop_task

log = logging.getLogger(__name__)


THUMBNAIL_TRUNCATE_FILE_SIZE = 32 * (2 ** 20)  # 32 MB
"""On files larger than this limit, truncate them when sending.
This ensures thumbnail generation doesn't clog up our pipeline,
instead preferring to fail after 50/300MB for huge PDFs/Words."""


THUMBNAIL_MIME_TYPES = {
    'application/postscript',
    'image/x-jg',
    'image/x-ms-bmp',
    'image/x-canon-cr2',
    'image/x-canon-crw',
    'application/dicom',
    'application/x-director',
    'image/vnd.djvu',
    'application/msword',
    'image/x-epson-erf',
    'image/gif',
    'image/heic',
    'image/vnd.microsoft.icon',
    'application/x-info',
    'image/x-jng',
    'image/jp2',
    'image/jpeg',
    'image/jpm',
    'image/x-nikon-nef',
    'image/x-olympus-orf',
    'application/font-sfnt',
    'image/x-portable-bitmap',
    'image/pcx',
    'application/x-font',
    'image/x-portable-graymap',
    'image/png',
    'image/x-portable-anymap',
    'image/x-portable-pixmap',
    'image/x-photoshop',
    'image/x-cmu-raster',
    'image/x-rgb',
    'image/tiff',
    'application/vnd.visio',
    'image/vnd.wap.wbmp',
    'application/x-ms-wmz',
    'image/x-xbitmap',
    'application/x-xcf',
    'image/x-xpixmap',
    'image/x-xwindowdump',
    'image/png',
    'application/postscript',
    'image/x-eps',
    'image/x-jg',
    'image/x-sony-arw',
    'image/x-ms-bmp',
    'image/x-canon-cr2',
    'image/x-canon-crw',
    'application/dicom',
    'image/x-kodak-dcr',
    'image/vnd.djvu',
    'image/x-adobe-dng',
    'application/msword',
    'image/x-epson-erf',
    'image/gif',
    'image/vnd.microsoft.icon',
    'application/x-info',
    'image/x-jng',
    'image/jp2',
    'image/jpeg',
    'image/jpm',
    'application/json',
    'image/x-kodak-k25',
    'image/x-kodak-kdc',
    'image/x-minolta-mrw',
    'image/x-nikon-nef',
    'image/x-olympus-orf',
    'application/font-sfnt',
    'image/x-portable-bitmap',
    'image/pcx',
    'image/x-pentax-pef',
    'application/x-font',
    'image/x-portable-graymap',
    'image/png',
    'image/x-portable-anymap',
    'image/x-portable-pixmap',
    'image/x-photoshop',
    'image/x-fuji-raf',
    'image/x-cmu-raster',
    'image/x-panasonic-raw',
    'image/x-rgb',
    'image/x-panasonic-rw2',
    'image/x-sony-sr2',
    'image/x-sony-srf',
    'image/tiff',
    'application/vnd.visio',
    'image/vnd.wap.wbmp',
    'application/x-ms-wmz',
    'image/x-sigma-x3f',
    'image/x-xbitmap',
    'application/x-xcf',
    'image/x-xpixmap',
    'image/x-xwindowdump',
    'image/x-sony-arw',
    'image/x-adobe-dng',
    'image/x-sony-sr2',
    'image/x-sony-srf',
    'image/x-sigma-x3f',
    'image/x-canon-crw',
    'image/x-canon-cr2',
    'image/x-epson-erf',
    'image/x-fuji-raf',
    'image/x-nikon-nef',
    'image/x-olympus-orf',
    'image/x-panasonic-raw',
    'image/x-panasonic-rw2',
    'image/x-pentax-pef',
    'image/x-kodak-dcr',
    'image/x-kodak-k25',
    'image/x-kodak-kdc',
    'image/x-minolta-mrw',
    'application/x-xcf',
    'image/x-xcf',
    'image/svg+xml',
    'image/svg',
    'image/svg+xml',
    'image/svg',
    'application/vnd.scribus',
    'application/vnd.oasis.opendocument.chart',
    'application/vnd.oasis.opendocument.chart-template',
    'application/vnd.oasis.opendocument.formula',
    'application/vnd.oasis.opendocument.formula-template',
    'application/vnd.oasis.opendocument.graphics',
    'application/vnd.oasis.opendocument.graphics-template',
    'application/vnd.oasis.opendocument.graphics-flat-xml',
    'application/vnd.oasis.opendocument.presentation',
    'application/vnd.oasis.opendocument.presentation-template',
    'application/vnd.oasis.opendocument.presentation-flat-xml',
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.spreadsheet-template',
    'application/vnd.oasis.opendocument.spreadsheet-flat-xml',
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.text-flat-xml',
    'application/vnd.oasis.opendocument.text-master',
    'application/vnd.oasis.opendocument.text-template',
    'application/vnd.oasis.opendocument.text-master-template',
    'application/vnd.oasis.opendocument.text-web',
    'application/vnd.sun.xml.calc',
    'application/vnd.sun.xml.calc.template',
    'application/vnd.sun.xml.chart',
    'application/vnd.sun.xml.draw',
    'application/vnd.sun.xml.draw.template',
    'application/vnd.sun.xml.impress',
    'application/vnd.sun.xml.impress.template',
    'application/vnd.sun.xml.math',
    'application/vnd.sun.xml.writer',
    'application/vnd.sun.xml.writer.global',
    'application/vnd.sun.xml.writer.template',
    'application/vnd.sun.xml.writer.web',
    'application/msword',
    'application/vnd.ms-powerpoint',
    'application/vnd.ms-excel',
    'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
    'application/vnd.ms-excel.sheet.macroEnabled.12',
    'application/vnd.ms-excel.template.macroEnabled.12',
    'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
    'application/vnd.ms-powerpoint.slide.macroEnabled.12',
    'application/vnd.ms-powerpoint.slideshow.macroEnabled.12',
    'application/vnd.ms-powerpoint.template.macroEnabled.12',
    'application/vnd.ms-word.document.macroEnabled.12',
    'application/vnd.ms-word.template.macroEnabled.12',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.openxmlformats-officedocument.presentationml.template',
    'application/vnd.openxmlformats-officedocument.presentationml.slideshow',
    'application/vnd.openxmlformats-officedocument.presentationml.slide',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
    'application/vnd.visio',
    'application/visio.drawing',
    'application/vnd.visio2013',
    'application/vnd.visio.xml',
    'application/x-mspublisher',
    'application/wps-office.doc',
    'application/wps-office.docx',
    'application/wps-office.xls',
    'application/wps-office.xlsx',
    'application/wps-office.ppt',
    'application/wps-office.pptx',
    'application/xhtml+xml',
    'application/mathml+xml',
    'text/html',
    'application/docbook+xml',
    'text/spreadsheet',
    'application/x-qpro',
    'application/x-dbase',
    'application/vnd.corel-draw',
    'application/vnd.lotus-wordpro',
    'application/vnd.lotus-1-2-3',
    'application/vnd.wordperfect',
    'application/wordperfect5.1',
    'application/vnd.ms-works',
    'application/clarisworks',
    'application/macwriteii',
    'application/vnd.apple.keynote',
    'application/vnd.apple.numbers',
    'application/vnd.apple.pages',
    'application/x-iwork-keynote-sffkey',
    'application/x-iwork-numbers-sffnumbers',
    'application/x-iwork-pages-sffpages',
    'application/x-hwp',
    'application/x-aportisdoc',
    'application/prs.plucker',
    'application/vnd.palm',
    'application/x-sony-bbeb',
    'application/x-pocket-word',
    'application/x-t602',
    'application/x-fictionbook+xml',
    'application/x-abiword',
    'application/x-pagemaker',
    'application/x-gnumeric',
    'application/vnd.stardivision.calc',
    'application/vnd.stardivision.draw',
    'application/vnd.stardivision.writer',
    'application/x-starcalc',
    'application/x-stardraw',
    'application/x-starwriter',
    'image/x-freehand',
    'image/cgm',
    'image/tif',
    'image/tiff',
    'image/vnd.dxf',
    'image/emf',
    'image/x-emf',
    'image/x-targa',
    'image/x-sgf',
    'image/x-svm',
    'image/wmf',
    'image/x-wmf',
    'image/x-pict',
    'image/x-cmx',
    'image/x-wpg',
    'image/x-eps',
    'image/x-met',
    'image/x-portable-bitmap',
    'image/x-photo-cd',
    'image/x-pcx',
    'image/x-portable-graymap',
    'image/x-portable-pixmap',
    'image/vnd.adobe.photoshop',
    'image/x-cmu-raster',
    'image/x-sun-raster',
    'image/x-xbitmap',
    'image/x-xpixmap',
    'application/sla',
    'application/vnd.ms-pki.stl',
    'application/x-navistyle',
    'model/stl',
    'application/wobj',
    'application/object',
    'model/obj',
    'application/ply',
    'application/pdf',
    'application/x-videolan',
    'video/3gpp',
    'video/annodex',
    'video/dl',
    'video/dv',
    'video/fli',
    'video/gl',
    'video/mpeg',
    'video/mp2t',
    'video/mp4',
    'video/quicktime',
    'video/mp4v-es',
    'video/ogg',
    'video/parityfec',
    'video/pointer',
    'video/webm',
    'video/vnd.fvt',
    'video/vnd.motorola.video',
    'video/vnd.motorola.videop',
    'video/vnd.mpegurl',
    'video/vnd.mts',
    'video/vnd.nokia.interleaved-multimedia',
    'video/vnd.vivo',
    'video/x-flv',
    'video/x-la-asf',
    'video/x-mng',
    'video/x-ms-asf',
    'video/x-ms-wm',
    'video/x-ms-wmv',
    'video/x-ms-wmx',
    'video/x-ms-wvx',
    'video/x-msvideo',
    'video/x-sgi-movie',
    'video/x-matroska',
    'video/x-theora+ogg',
    'video/x-m4v',
}
"""List of mime types, that the thumbnail service supports.
Based on [[https://github.com/algoo/preview-generator/blob/develop/doc/supported_mimetypes.rst]]
"""

TIMEOUT_BASE = 60
"""Minimum number of seconds to wait for this service."""

TIMEOUT_MAX = 400
"""Maximum number of seconds to wait for this service."""

MIN_SPEED_BPS = 10 * 1024  # 10 KB/s
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def can_create(blob):
    """Checks if thumbnail generator service can process this mime type."""
    if blob.mime_type in THUMBNAIL_MIME_TYPES and blob.size < THUMBNAIL_TRUNCATE_FILE_SIZE:
        return True

    return False


def call_thumbnails_service(blob, size):
    """Executes HTTP PUT request to Thumbnail service.

    Args:
        data: the file for which a thumbnail will be created.
        size: the size for the created thumbnail (thumbnail will be size x size)
        """

    url = settings.SNOOP_THUMBNAIL_URL + f'preview/{size}x{size}'
    actual_size = min(blob.size, THUMBNAIL_TRUNCATE_FILE_SIZE)
    timeout = min(TIMEOUT_MAX, int(TIMEOUT_BASE + actual_size / MIN_SPEED_BPS))

    # instead of streaming the file, just read some 50MB into a bytes string and send that, capping out
    # the data sent per file for this very slow service.

    with blob.open() as f:
        data = utils.read_exactly(f, THUMBNAIL_TRUNCATE_FILE_SIZE)
        payload = {'file': data}

    try:
        resp = requests.post(url, files=payload, timeout=timeout)
    except Exception as e:
        log.exception(e)
        raise SnoopTaskBroken('timeout and/or connection error, timeout = ' + str(round(timeout)) + 's',
                              'thumbnail_timeout')

    if (resp.status_code != 200
            or resp.headers['Content-Type'] != 'image/jpeg'):
        raise SnoopTaskBroken(resp.text, 'thumbnail_http_' + str(resp.status_code))

    return resp.content


@snoop_task('thumbnails.get_thumbnail', version=5)
# the @returns_json_blob decorator is only needed to check if this function ran in digests.gather
@returns_json_blob
def get_thumbnail(blob, pdf_preview=None):
    """Function that calls the thumbnail service for a given blob.

    Args:
        blob: Original file that we need a thumbnail for
        source: If set, will use this data for the actual creation of the thumbnail.
                Useful if we have PDF conversions.
    """

    if pdf_preview and isinstance(pdf_preview, models.Blob) and pdf_preview.size > 0:
        source = pdf_preview
    else:
        source = blob

    for size in models.Thumbnail.SizeChoices.values:
        resp = call_thumbnails_service(source, size)
        blob_thumb = models.Blob.create_from_bytes(resp)
        _, _ = models.Thumbnail.objects.update_or_create(
            blob=blob,
            size=size,
            defaults={'thumbnail': blob_thumb, 'source': source}
        )

    return True
