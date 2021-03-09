import sys
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models
from ... import collections
from django.db.models import Count
from django.db.models import Sum
import re
import mimetypes

SUPPORTED_FILETYPES = {
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

def truncateSize(size):
    exponent = len(str(size))-1
    divisor = 10**exponent
    return int((size/divisor))*divisor

class Command(BaseCommand):
    help = "Display filetype stats."

    def add_arguments(self, parser):
        parser.add_argument('collection', type=str)

    def handle(self, collection, **options):

        col = collections.ALL[collection]
        
        with col.set_current():
            querysetMime = models.Blob.objects.all().values('mime_type').annotate(total=Count('mime_type')).annotate(size=Sum('size')).order_by('-size')
            print()
            print('---- Top 100 Mime-Types by size ----')
            print()
            for mtype in querysetMime:
                print(f'{(mtype["mime_type"]):75} {truncateSize(mtype["size"]):10d}')

            print()
            print('---- Top Unsupported Mime-Types ----')
            print()
            unsuppQuery = querysetMime.exclude(mime_type__in=SUPPORTED_FILETYPES)
            for mtype in unsuppQuery[:100]:
                #if mtype['mime_type'] not in SUPPORTED_FILETYPES:
                extensions = mimetypes.guess_all_extensions(mtype['mime_type'])
                print(f'{(mtype["mime_type"]):75} {truncateSize(mtype["size"]):10d} Extensions: {extensions}')
            
            querysetExt = models.File.objects.all().values('name_bytes', 'size').order_by('-size')
            extDict = {}
            for filename in querysetExt:
                filestr = str(filename['name_bytes'],'utf8')

                try:
                    fileext = re.match(r'.*?(\..*)', filestr)[1]
                except TypeError:
                    continue

                if fileext not in extDict:
                    extDict[fileext] = filename['size'] 
                else:
                    extDict[fileext] += filename['size']

            print()
            print('---- Top 100 File-Extensions By Size ----')
            print()
            for ext,size in sorted(extDict.items(), key=lambda x: x[1], reverse=True)[:100]:

                print(f'{ext:75} {truncateSize(size):10d}')
