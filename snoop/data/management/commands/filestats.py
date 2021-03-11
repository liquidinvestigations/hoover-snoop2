from django.core.management.base import BaseCommand
from ... import models
from ... import collections
from django.db.models import Count
from django.db.models import Sum
from django.db import connections

from ...analyzers import archives
from ...analyzers import tika
from ...analyzers import email
from ...analyzers import exif
from ...analyzers import html
from ... import filesystem

SUPPORTED_FILETYPES = (archives.KNOWN_TYPES
                       .union(set(tika.TIKA_CONTENT_TYPES))
                       .union(filesystem.EMLX_EMAIL_MIME_TYPE)
                       .union(email.OUTLOOK_POSSIBLE_MIME_TYPES)
                       .union(filesystem.RFC822_EMAIL_MIME_TYPE)
                       .union(exif.EXIFREAD_FILETYPES)
                       .union(html.HTML_MIME_TYPES))

SUPPORTED_FILETYPES_OLD = {
    'application/x-7z-compressed',
    'application/zip',
    'application/x-zip',
    'application/x-rar',
    'application/rar',
    'application/x-gzip',
    'application/gzip',
    'application/x-bzip2',
    'application/x-tar',
    'application/x-hoover-pst', 
    'application/mbox',
    'application/pdf',

    'text/plain',
    'text/html',
    'text/rtf',

    'application/pdf',

    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
    'application/vnd.ms-word.document.macroEnabled.12',
    'application/vnd.ms-word.template.macroEnabled.12',
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.text-template',
    'application/rtf',

    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
    'application/vnd.ms-excel.sheet.macroEnabled.12',
    'application/vnd.ms-excel.template.macroEnabled.12',
    'application/vnd.ms-excel.addin.macroEnabled.12',
    'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
    'application/vnd.oasis.opendocument.spreadsheet-template',
    'application/vnd.oasis.opendocument.spreadsheet',

    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.openxmlformats-officedocument.presentationml.template',
    'application/vnd.openxmlformats-officedocument.presentationml.slideshow',
    'application/vnd.ms-powerpoint',
    'application/vnd.ms-powerpoint.addin.macroEnabled.12',
    'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
    'application/vnd.ms-powerpoint.template.macroEnabled.12',
    'application/vnd.ms-powerpoint.slideshow.macroEnabled.12',
    'application/vnd.oasis.opendocument.presentation',
    'application/vnd.oasis.opendocument.presentation-template',

    'application/xhtml+xml',
    'application/xml',
    'text/xml',
    'image/tiff',
    'image/jpg',
    'image/webp',
    'image/heic',

    'application/vnd.ms-outlook',
    'application/vnd.ms-office',
    'application/CDFV2',
    'message/rfc822',
    'message/x-emlx',
}


def truncate_size(size):
    return round(size, -((len(str(size))) - 1))


def get_top_mime_types(collections_list=collections.ALL, print_supported=True):
    for col in collections_list:
        res = {}
        querysetMime = models.Blob.objects.all().values('mime_type').annotate(total=Count('mime_type')).annotate(size=Sum('size')).order_by('-size')
        if not print_supported:
            querysetMime = querysetMime.exclude(mime_type__in=SUPPORTED_FILETYPES)
        collection = collections.ALL[col]
        with collection.set_current():
            for mtype in querysetMime:
                if mtype['mime_type'] not in res:
                    res[mtype['mime_type']] = truncate_size(mtype['size'])
                else:
                    res[mtype['mime_type']] += truncate_size(mtype['size'])
    return res


def get_top_extensions(collections_list=collections.ALL, print_supported=True):
    for col in collections_list:
        query = """select substring(encode(f.name_bytes::bytea, 'escape')::text
                    from '(\..{1,20})$') as ext,
                    sum(f.size) as size,
                    b.mime_type as mime
                    from data_file f
                    join data_blob b on f.blob_id = b.sha3_256
                    group by ext, mime
                    order by size desc limit 100;"""
        with connections['collection_' + col].cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        extDict = {}
        for entry in results:
            if not print_supported:
                if entry[2] in SUPPORTED_FILETYPES:
                    continue
            if entry[0] not in extDict:
                extDict[entry[0]] = {'size': truncate_size(int(entry[1])), 'mtype': set([entry[2]])}
            else:
                extDict[entry[0]]['size'] += truncate_size(int(entry[1]))
                extDict[entry[0]]['mtype'].add(entry[2])
    return extDict


class Command(BaseCommand):
    help = "Display filetype stats."

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)

    def handle(self, **options):
        for k, v in get_top_mime_types(print_supported=False).items():
            print(k, v)
        for k, v in get_top_extensions(print_supported=False).items():
            print(k, v)
