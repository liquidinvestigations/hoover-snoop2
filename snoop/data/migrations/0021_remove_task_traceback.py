# Generated by Django 2.0 on 2018-04-24 10:17

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0020_task_log'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='task',
            name='traceback',
        ),
    ]
