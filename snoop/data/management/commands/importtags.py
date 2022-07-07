import csv
import logging

from ... import models, collections

from django.core.management.base import BaseCommand
from snoop.data.logs import logging_for_management_command

log = logging.getLogger(__name__)


def clean_tag(tag):
    """Remove whitespace from imported tag"""
    tag_stripped = tag.strip()
    cleaned_tag = tag_stripped.translate(str.maketrans({' ': '_', '\n': '_', '\t': '_', '\r': '_'}))
    return cleaned_tag


def read_csv(file_path):
    """Read a csv containing md5 hashes and tags to import.

    Returns: dictionary with md5 hashes as keys and a list of tags as values.
    """
    taglist = {}
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, fieldnames=['md5', 'tags'])
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            clean_tags = row['tags'].split(',')
            clean_tags = [clean_tag(tag) for tag in clean_tags]
            taglist[row['md5']] = clean_tags
    return taglist


def update_tags(md5, tags, collection, uuid):
    """Create document tags for all new tags from the list."""
    collection = collections.ALL[collection]
    with collection.set_current():
        blob = models.Blob.objects.get(md5=md5)
        digest = models.Digest.objects.get(blob=blob.pk)
        existing_tags = models.DocumentUserTag.objects.filter(digest=digest.pk).values_list('tag', flat=True)
        models.DocumentUserTag.objects.create(digest=digest, uuid=uuid, tag=' abc ',
                                              user='kjell', public=False)
        for tag in tags:
            if tag not in existing_tags:
                models.DocumentUserTag.objects.create(digest=digest, uuid=uuid, tag=tag,
                                                      user='kjell', public=False)


class Command(BaseCommand):
    "Import Tags UUIDs for all collections. JSON content is read from stdin."

    def add_arguments(self, parser):
        parser.add_argument('collection', help='collection name')
        parser.add_argument('taglist', help='Path to csv with tags.')
        parser.add_argument('uuid', help='UUID of user for which the tags are imported.')

    def handle(self, taglist, collection, uuid, **options):
        logging_for_management_command(options['verbosity'])
        for md5, taglist in read_csv(taglist).items():
            update_tags(md5, taglist, collection, uuid)
