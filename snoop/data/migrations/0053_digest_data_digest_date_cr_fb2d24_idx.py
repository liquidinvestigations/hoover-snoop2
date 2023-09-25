# Generated by Django 3.2.15 on 2023-09-12 20:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0052_remove_archive_mount_fields'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='digest',
            index=models.Index(fields=['date_created'], name='data_digest_date_cr_fb2d24_idx'),
        ),
    ]