"""Clear caches using this management command"""
from django.conf import settings
from django.core.cache import caches

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Clear cache"

    def handle(self, **options):
        """Clear cache"""
        for k in settings.CACHES.keys():
            caches[k].clear()
            self.stderr.write("Cleared cache '{}'.\n".format(k))
