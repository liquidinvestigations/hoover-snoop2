from pathlib import Path
from django.core.management.base import BaseCommand
from ...models import Collection


class Command(BaseCommand):
    help = "Creates a collection"

    def add_arguments(self, parser):
        parser.add_argument('name')
        parser.add_argument('root', type=Path)

    def handle(self, *args, **options):
        col = Collection.objects.create(name=options['name'], root=options['root'])
        root = col.directory_set.create()
