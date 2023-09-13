"""Models that should be in the default db, not in the specific collection db."""

from django.db import models

from psqlextra.types import PostgresPartitioningMethod
from psqlextra.models import PostgresPartitionedModel
from psqlextra.indexes import UniqueIndex


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
