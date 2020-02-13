# Generated by Django 2.2.8 on 2020-02-12 20:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0029_auto_20181019_1421'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='task',
            index=models.Index(fields=['status', 'date_modified'], name='data_task_status_6689af_idx'),
        ),
        migrations.AddIndex(
            model_name='task',
            index=models.Index(fields=['func', 'date_modified'], name='data_task_func_bd935b_idx'),
        ),
    ]