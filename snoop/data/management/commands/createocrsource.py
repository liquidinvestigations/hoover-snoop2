from pathlib import Path
from django.core.management.base import BaseCommand
from ... import ocr


class Command(BaseCommand):
    help = "Creates an OCR source"

    def add_arguments(self, parser):
        parser.add_argument('name')
        parser.add_argument('root', type=Path)

    def handle(self, *args, **options):
        ocr.create_ocr_source(
            name=options['name'],
            root=options['root'].resolve(),
        )
