"""Command to import tags from a file.
"""
import csv
import logging
import traceback
import sys

from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command

from ... import collections, models

log = logging.getLogger(__name__)


def clean_tag(tag):
    """Remove whitespace from imported tag"""
    tag_stripped = tag.strip()
    cleaned_tag = tag_stripped.translate(str.maketrans({' ': '_', '\n': '_', '\t': '_', '\r': '_'}))
    return cleaned_tag


def read_csv():
    """Read a csv from stdin containing md5 hashes and tags to import.

    Returns: dictionary with md5 hashes as keys and a list of tags as values.
    """
    csv_file = sys.stdin
    taglist = {}
    csv_reader = csv.DictReader(csv_file, fieldnames=['md5', 'tags'])
    next(csv_reader, None)  # skip the headers
    for row in csv_reader:
        clean_tags = row['tags'].split(',')
        clean_tags = [clean_tag(tag) for tag in clean_tags]
        taglist[row['md5']] = clean_tags
    return taglist


def update_tags(md5, tags, collection, uuid, username, public=False):
    """Create document tags for all new tags from the list."""
    updated = False
    with collection.set_current():
        blob = models.Blob.objects.get(md5=md5)
        digest = models.Digest.objects.get(blob=blob.pk)
        updated = False
        for tag in tags:
            _, created = models.DocumentUserTag.objects.get_or_create(digest=digest, uuid=uuid, tag=tag,
                                                                      user=username, public=public)
            if created:
                updated = True
                log.info(f'Created new tag: "{tag}" for document: "{md5}"')
        return updated


class Command(BaseCommand):
    "Import Tags UUIDs for all collections. JSON content is read from stdin."

    def add_arguments(self, parser):
        parser.add_argument('-c', '--collection', help='collection name', required=True)
        parser.add_argument('--uuid', help='UUID of user for which the tags are imported.', required=True)
        parser.add_argument('--user', help='Username matching the UUID.', required=True)
        parser.add_argument('-p', '--public', action='store_true', help='Flag to set the tags as public.',
                            required=True)

    def handle(self, **options):
        logging_for_management_command(options['verbosity'])
        collection_name = options.get('collection')
        try:
            collection = collections.ALL[collection_name]
        except KeyError:
            log.info(f'Collection: "{collection_name}" does not exists.')
            log.info('Exiting!')
            return
        updated_any = False
        try:
            for md5, tags in read_csv().items():
                updated = update_tags(md5, tags, collection, options.get('uuid'),
                                      options.get('user'), options.get('public'))
                if not updated_any and updated:
                    updated_any = True

            if not updated_any:
                log.info('Found no new tags to update!')

        except Exception as e:
            log.error(f'Error updating tags: {e}')
            log.error(traceback.format_exc())
