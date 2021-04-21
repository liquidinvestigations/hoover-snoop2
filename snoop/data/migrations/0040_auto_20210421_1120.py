# Generated by Django 3.1.4 on 2021-04-21 11:20

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0039_auto_20210204_2125'),
    ]

    operations = [
        migrations.CreateModel(
            name='Thumbnail',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('size', models.IntegerField(choices=[(100, 'Small'), (200, 'Medium'), (400, 'Large')], default=200)),
                ('blob', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='file_blob', to='data.blob')),
                ('thumbnail', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='thumbnail_blob', to='data.blob')),
            ],
        ),
        migrations.AddConstraint(
            model_name='thumbnail',
            constraint=models.UniqueConstraint(fields=('blob', 'size'), name='unique_size'),
        ),
    ]
