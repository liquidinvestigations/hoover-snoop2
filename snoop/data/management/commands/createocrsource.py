from pathlib import Path
from django.core.management.base import BaseCommand
from ... import ocr


class Command(BaseCommand):
    help = "Creates an OCR source"

    def add_arguments(self, parser):
        parser.add_argument('name', help="OCR source name.")
        parser.add_argument('root', type=Path, help="Valid filesystem path.")

    def handle(self, *args, **options):
        ocr.create_ocr_source(
            name=options['name'],
        )
