from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('data', '0051_reset_unarchive_tasks.py'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='blob',
            name='archive_source_blob',
        ),
        migrations.RemoveField(
            model_name='blob',
            name='archive_source_key',
        ),
    ]
