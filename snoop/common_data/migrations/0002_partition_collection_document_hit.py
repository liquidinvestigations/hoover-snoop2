from django.db import migrations, models

from psqlextra.backend.migrations.operations import PostgresAddHashPartition


# each row of this table takes about 1KB on the database.
# Under the biggest load we have, 50mil documents = 1gb/partition
# that means about 25m rows/partition.
COLLECTION_DOCUMENT_HIT_PARTITION_COUNT = 50


class Migration(migrations.Migration):
    dependencies = [
        ('common_data', '0001_initial'),
    ]
    operations = [
        PostgresAddHashPartition(
            model_name="CollectionDocumentHit",
            name=f"pt_sha3_{i+1}_of_{COLLECTION_DOCUMENT_HIT_PARTITION_COUNT}",
            modulus=COLLECTION_DOCUMENT_HIT_PARTITION_COUNT,
            remainder=i,
        )
        for i in range(COLLECTION_DOCUMENT_HIT_PARTITION_COUNT)
    ]
