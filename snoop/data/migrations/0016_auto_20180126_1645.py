# Generated by Django 2.0 on 2018-01-26 14:45

from django.db import migrations, models


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0015_auto_20180125_1605'),
    ]

    operations = [
        migrations.AddField(
            model_name='blob',
            name='size',
            field=models.BigIntegerField(default=-1),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='file',
            name='size',
            field=models.BigIntegerField(),
        ),
    ]
