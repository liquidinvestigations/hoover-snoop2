# Generated by Django 2.0 on 2018-01-19 13:32

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0013_auto_20180119_1318'),
    ]

    operations = [
        migrations.AddField(
            model_name='file',
            name='blob',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                    to='data.Blob'),
        ),
    ]
