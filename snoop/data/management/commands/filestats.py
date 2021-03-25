"""Command to get statistics for filetypes that exist in collections."""

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


SUPPORTED_MIME_TYPES = (archives.ARCHIVES_MIME_TYPES
                        .union(tika.TIKA_MIME_TYPES)
                        .union(filesystem.EMLX_EMAIL_MIME_TYPES)
                        .union(email.OUTLOOK_POSSIBLE_MIME_TYPES)
                        .union(filesystem.RFC822_EMAIL_MIME_TYPES)
                        .union(exif.EXIFREAD_MIME_TYPES)
                        .union(html.HTML_MIME_TYPES))


def truncate_size(size):
    """Generate a truncated number for a given number.

    This is needed to anonymize the statistics, so they can't be traced back
    to some dataset.
    """
    return round(size, -((len(str(size))) - 1))


def get_top_mime_types(collections_list, print_supported=True):
    """Return a dictionary of mime-types that occupy most space in collections.

    Args:
        collections_list: A list of collections that will be analyzed.
        print_supported: When False only analyzes unsupported filetypes.
    """
    res = {}
    for col in collections_list:
        collection = collections.ALL[col]
        with collection.set_current():
            queryset_mime = models.Blob.objects.all().values('mime_type', 'magic')\
                .annotate(total=Count('mime_type')).annotate(size=Sum('size'))\
                .order_by('-size')
            if not print_supported:
                queryset_mime = queryset_mime.exclude(mime_type__in=SUPPORTED_MIME_TYPES)
            for mtype in queryset_mime:
                if mtype['mime_type'] not in res:
                    res[mtype['mime_type']] = {'size': truncate_size(mtype['size']),
                                               'magic': get_description(col, mtype['mime_type'])}
                else:
                    res[mtype['mime_type']]['size'] += truncate_size(mtype['size'])
    sorted_res = sorted(res.items(), key=lambda x: x[1]['size'], reverse=True)
    return dict(sorted_res)


def get_top_extensions(collections_list, print_supported=True):
    """Return a dictionary of file extensions that occupy most space in collections.

    Args:
        collections_list: A list of collections that will be analyzed.
        print_supported: When False only analyzes unsupported filetypes.
    """
    ext_dict = {}
    for col in collections_list:
        query = r"""select substring(encode(f.name_bytes::bytea, 'escape')::text
                    from '(\..{1,20})$') as ext,
                    sum(f.size) as size,
                    b.mime_type as mime
                    from data_file f
                    join data_blob b on f.blob_id = b.sha3_256
                    group by ext, mime
                    order by size desc limit 100;"""
        with connections[collections.ALL[col].db_alias].cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        for ext, size, mime in results:
            if not print_supported:
                if mime in SUPPORTED_MIME_TYPES:
                    continue
            if ext not in ext_dict:
                ext_dict[ext] = {'size': truncate_size(int(size)), 'mtype': set([mime])}
            else:
                ext_dict[ext]['size'] += truncate_size(int(size))
                ext_dict[ext]['mtype'].add(mime)
    sorted_ext_dict = sorted(ext_dict.items(), key=lambda x: x[1]['size'], reverse=True)
    return dict(sorted_ext_dict)


def get_description(col, mime_type, *extension):
    """Return the magic description for a given mime-type.

    Args:
        col: Collection on which the query is executed.
        mime_type: Mime-Type for which the descriptions is returned.
        *extension: Optional file-extension as string to limit the search to this
            extension.
    """

    collection = collections.ALL[col]
    with collection.set_current():
        try:
            if extension:
                queryset = models.File.objects\
                    .annotate(str_name=RawSQL("encode(name_bytes::bytea, 'escape')::text", ()))\
                    .filter(blob__mime_type=mime_type, str_name__endswith=extension[0])\
                    .values("blob__magic")[0]
            else:
                extension = [""]
                queryset = models.File.objects\
                    .annotate(str_name=RawSQL("encode(name_bytes::bytea, 'escape')::text", ()))\
                    .filter(blob__mime_type=mime_type)\
                    .values("blob__magic")[0]
        except IndexError:
            return None
        return queryset['blob__magic']


class Command(BaseCommand):
    """Print the statistics for mimetypes or file-extendsion"""
    help = "Display filetype stats."

    def add_arguments(self, parser):
        """Arguments to show only unsupported types, include magic descriptions,
        include full magic descriptions and for choosing specific collections"""

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
        """Prints out the Top 100 (or so) mime-types and file extensions:

        Results are sorted by total file size usage.
        """
        collection_args = list(collections.ALL.keys())
        supported = True
        unsupp_str = ' '
        if options['unsupported']:
            supported = False
            unsupp_str = ' Unsupported '
        if options['collections']:
            collection_args = options['collections']

        print(f'Top{unsupp_str}Mime Types by size')
        print('-----------------------')
        for k, v in get_top_mime_types(collections_list=collection_args, print_supported=supported).items():
            size = v['size'] / (2 ** 20)
            if options['descriptions']:
                print(f'{k:50} {size:10,.2f} MB {str(v["magic"]):{100}.{100}}')
            elif options['full_descriptions']:
                print(f'{k:50} {size:10,.2f} MB {str(v["magic"])}')
            else:
                print(f'{k:50} {size:10,.2f} MB')

        print()
        print(f'Top{unsupp_str}File Extensions by size')
        print('-----------------------')
        for k, v in get_top_extensions(collections_list=collection_args, print_supported=supported).items():
            size = v['size'] / (2 ** 20)
            print(f'{str(k):22} {size:10,.2f} MB {", ".join(v["mtype"])}')
