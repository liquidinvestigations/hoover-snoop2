# Generated by Django 2.0.4 on 2018-05-09 09:36

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0025_auto_20180509_0933'),
    ]

    operations = [
        migrations.AlterField(
            model_name='taskdependency',
            name='next',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                    related_name='prev_set', to='data.Task'),
        ),
        migrations.AlterField(
            model_name='taskdependency',
            name='prev',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE,
                                    related_name='next_set', to='data.Task'),
        ),
    ]
