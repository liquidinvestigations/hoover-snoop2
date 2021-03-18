"""Dump an object from object storage to standard output.

The object (Blob) is fetched by its SHA3-256 primary key.

Can be used by third parties to export data; is not used internally.
"""
import sys
from django.core.management.base import BaseCommand
from ...logs import logging_for_management_command
from ... import models


class Command(BaseCommand):
    help = "Write blobs to stdout."

    def add_arguments(self, parser):
        parser.add_argument('blob_id', type=str, help="SHA3-256 based blob ID.")

    def handle(self, *args, **options):
        logging_for_management_command()

        if options['blob_id']:
            with models.Blob.objects.get(pk=options['blob_id']).open() as f:
                sys.stdout.buffer.write(f.read())
                sys.stdout.flush()
