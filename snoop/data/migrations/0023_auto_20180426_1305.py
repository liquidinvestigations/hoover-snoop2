# Generated by Django 2.0.4 on 2018-04-26 13:05

from django.db import migrations, models


def name_to_bytes(apps, schema_editor):
    db_alias = schema_editor.connection.alias
    File = apps.get_model('data', 'File')
    for file in File.objects.using(db_alias).all():
        file.name_bytes = file.name.encode('utf8')
        file.save()

    Directory = apps.get_model('data', 'Directory')
    for directory in Directory.objects.using(db_alias).all():
        directory.name_bytes = directory.name.encode('utf8')
        directory.save()


def name_from_bytes(apps, schema_editor):
    db_alias = schema_editor.connection.alias
    File = apps.get_model('data', 'File')
    for file in File.objects.using(db_alias).all():
        file.name = file.name_bytes.decode('utf8')
        file.save()

    Directory = apps.get_model('data', 'Directory')
    for directory in Directory.objects.using(db_alias).all():
        directory.name = directory.name_bytes.decode('utf8')
        directory.save()


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0022_auto_20180426_1248'),
    ]

    operations = [
        migrations.AddField(
            model_name='directory',
            name='name_bytes',
            field=models.BinaryField(blank=True, max_length=1024),
        ),
        migrations.AddField(
            model_name='file',
            name='name_bytes',
            field=models.BinaryField(blank=True, max_length=1024),
        ),
        migrations.RunPython(name_to_bytes, name_from_bytes),
    ]
