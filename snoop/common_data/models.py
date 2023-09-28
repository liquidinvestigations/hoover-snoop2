"""Models that should be in the default db, not in the specific collection db."""

import os
import subprocess
from django.conf import settings
from django.db import models

from contextlib import contextmanager
from snoop.data.collections import Collection
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


class NextcloudCollection(models.Model, Collection):
    """Model for storing nextcloud collection metadata."""

    name = models.CharField(max_length=256, unique=True)
    opt = models.JSONField(null=True, blank=True)

    @property
    def webdav_user(self):
        return self.opt.get('webdav_user', '')

    @property
    def webdav_password(self):
        return self.opt.get('webdav_password', '')

    @property
    def webdav_url(self):
        return self.opt.get('webdav_url', '')

    @contextmanager
    def mount_collection_webdav(self, col, nc_col):
        """Mount a nextcloud collection via webdav.
        """
        subprocess.run(['mkdir', '-p',
                        (
                            f'{settings.SNOOP_WEBDAV_MOUNT_DIR}'
                            f'/{self.name}/data'
                        )
                        ], check=True)
        secrets_content = (
            f'{settings.SNOOP_WEBDAV_MOUNT_DIR}'
            f'/{self.name}/data'
            f' {self.webdav_user} {self.webdav_password}'
        )
        with open('/etc/davfs2/secrets', 'a') as secrets_file:
            secrets_file.write(f'\n{secrets_content}')
            mount_command = (
                f'mount -t davfs http://10.66.60.1:9972{self.webdav_url} '
                f'{settings.SNOOP_WEBDAV_MOUNT_DIR}/{self.name}/data'
            )
            try:
                result = subprocess.run(mount_command, shell=True, check=True)
                print(result.returncode, result.stdout, result.stderr)
                print(f'Mounted collection {self.name}.')
            except subprocess.CalledProcessError as e:
                print(e, e.output)
                print(f'Could not mount collection {self.name}.')
        yield os.path.join(settings.SNOOP_WEBDAV_MOUNT_DIR, self.name, 'data')
