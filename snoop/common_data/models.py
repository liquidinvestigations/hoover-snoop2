"""Models that should be in the default db, not in the specific collection db."""

from django.db import models


class CollectionDocumentHit(models.Model):
    """Model to hold Collection <-> Document relationship.

    Useful to quickly de-duplicate cross-collections without making one
    database connection per collection."""

    collection_name = models.CharField(max_length=256, db_index=True)
    """name of collection where document is found"""

    doc_sha3_256 = models.CharField(max_length=64, db_index=True)
    """primary hash of the document, used as the Digest PK & search index id"""

    doc_date_added = models.DateTimeField(db_index=True)
    """Modification date of the Digest object. Used to incrementally pull new documents."""

    class Meta:
        unique_together = ('collection_name', 'doc_sha3_256')
