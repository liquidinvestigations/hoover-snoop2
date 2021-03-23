"""Command to create [snoop.data.models.OcrSource][].
"""

from pathlib import Path
from django.core.management.base import BaseCommand
from ... import ocr, collections


class Command(BaseCommand):
    """Creates an OCR source.

    Searches file at a pre-defined path, see [snoop.data.models.OcrSource][] for details.
    """
    help = "Creates an OCR source"

    def add_arguments(self, parser):
        parser.add_argument('collection', help="collection name")
        parser.add_argument('name', type=Path, help="OCR source name. "
                            "Files will be searched under $collections/$collection/ocr/$name.")

    def handle(self, collection, name, *args, **options):
        assert collection in collections.ALL, 'collection does not exist'
        with collections.ALL[collection].set_current():
            ocr.create_ocr_source(name=name)
