from django.core.management.base import BaseCommand
from ... import models
from ... import collections
from django.db.models import Count
from django.db.models import Sum
from django.db import connections
from django.db.models.expressions import RawSQL

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


def truncate_size(size):
    return round(size, -((len(str(size))) - 1))


def get_top_mime_types(collections_list=collections.ALL, print_supported=True):
    for col in collections_list:
        res = {}
        collection = collections.ALL[col]
        with collection.set_current():
            querysetMime = models.Blob.objects.all().values('mime_type', 'magic')\
                .annotate(total=Count('mime_type')).annotate(size=Sum('size'))\
                .order_by('-size')
            if not print_supported:
                querysetMime = querysetMime.exclude(mime_type__in=SUPPORTED_FILETYPES)
            for mtype in querysetMime:
                if mtype['mime_type'] not in res:
                    res[mtype['mime_type']] = {'size': truncate_size(mtype['size']),
                                               'magic': get_description(mtype['mime_type'], col)}
                else:
                    res[mtype['mime_type']]['size'] += truncate_size(mtype['size'])
    return res


def get_top_extensions(collections_list=collections.ALL, print_supported=True):
    for col in collections_list:
        query = r"""select substring(encode(f.name_bytes::bytea, 'escape')::text
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


def get_description(mime_type, col, extension=""):
    collection = collections.ALL[col]
    with collection.set_current():
        try:
            querySet = models.File.objects\
                .annotate(str_name=RawSQL("encode(name_bytes::bytea, 'escape')::text", ()))\
                .filter(blob__mime_type=mime_type, str_name__endswith=extension)\
                .values("blob__magic")[0]
        except IndexError:
            return None
    return querySet['blob__magic']


class Command(BaseCommand):
    help = "Display filetype stats."

    def add_arguments(self, parser):

        parser.add_argument(
            '--unsupported',
            action='store_true',
            help='exclude supported filetypes')

        parser.add_argument(
            '--descriptions',
            action='store_true',
            help='print MIME-type descriptions')

        parser.add_argument(
            '--full_descriptions',
            action='store_true',
            help='print full MIME-type descriptions')

        parser.add_argument(
            '--collections',
            nargs='+',
            type=str,
            help='specify collections')

    def handle(self, **options):
        collection_args = collections.ALL
        supported = True
        if options['unsupported']:
            supported = False
        if options['collections']:
            print(options['collections'])
            collection_args = options['collections']

        print('Top Mime Types by size')
        print('-----------------------')
        for k, v in get_top_mime_types(collections_list=collection_args, print_supported=supported).items():
            if options['descriptions']:
                print(f'{k:75} {v["size"]:12d} {str(v["magic"]):{100}.{100}}')
            elif options['full_descriptions']:
                print(f'{k:75} {v["size"]:12d} {str(v["magic"])}')
            else:
                print(f'{k:75} {v["size"]:12d}')

        print()
        print('Top File Types by size')
        print('-----------------------')
        for k, v in get_top_extensions(collections_list=collection_args, print_supported=supported).items():
            print(f'{str(k):75} {v["size"]:12d} {v["mtype"]}')
