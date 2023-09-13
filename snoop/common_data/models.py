"""Models that should be in the default db, not in the specific collection db."""

from django.db import models
import logging

from contextlib import contextmanager
from snoop.data.collections import Collection
from snoop.data.s3 import get_webdav_mount
from psqlextra.types import PostgresPartitioningMethod
from psqlextra.models import PostgresPartitionedModel
from psqlextra.indexes import UniqueIndex

logger = logging.getLogger(__name__)


class CollectionDocumentHit(PostgresPartitionedModel):
    """Model to hold Collection <-> Document relationship.

    Useful to quickly de-duplicate cross-collections without making one
    database connection per collection.

    Partition table by the doc hash, because we will want to fetch all the
    collections for a small list of docs (the search result page), so it makes
    sense to have that together.

    Partition count is controlled in the migrations."""

    collection_name = models.CharField(max_length=256, db_index=True)
    """name of collection where document is found"""

    doc_sha3_256 = models.CharField(max_length=64, db_index=True)
    """primary hash of the document, used as the Digest PK & search index id"""

    doc_date_added = models.DateTimeField(db_index=True)
    """Modification date of the Digest object. Used to incrementally pull new documents."""

    class Meta:
        indexes = [
            UniqueIndex(fields=['doc_sha3_256', 'collection_name']),
        ]

    class PartitioningMeta:
        method = PostgresPartitioningMethod.HASH
        key = ["doc_sha3_256"]


class NextcloudCollection(models.Model, Collection):
    """Model for storing nextcloud collection metadata."""

    name = models.CharField(max_length=256, unique=True)
    opt = models.JSONField(null=True, blank=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # django will call the constructor with a list of arguments
        # instead of keyword arguments
        if 'opt' not in kwargs:
            Collection.initialize(self, **args[2])
        else:
            Collection.initialize(self, **kwargs.get('opt'))

    @property
    def webdav_username(self):
        return self.opt.get('webdav_username', '')

    @property
    def webdav_password(self):
        return self.opt.get('webdav_password', '')

    @property
    def webdav_url(self):
        return self.opt.get('webdav_url', '')

    @contextmanager
    def mount_collections_root(self):
        """Mount a nextcloud collection via webdav.
        """
        yield get_webdav_mount(
            mount_name=f'{self.name}-collections',
            webdav_username=self.webdav_username,
            webdav_password=self.webdav_password,
            webdav_url=self.webdav_url,
        )

    def __repr__(self):
        """String representation for a Collection.
        """

        return f"<Collection {self.name} process={self.process} sync={self.sync}>"
