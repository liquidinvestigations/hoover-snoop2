from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Runs the dispatcher, which keeps collections up to date, by "
        "scanning the filesystem and launching processing jobs."
    )

    def handle(self, *args, **options):
        pass
