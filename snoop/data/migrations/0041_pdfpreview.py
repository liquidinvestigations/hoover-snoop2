# Generated by Django 3.1.4 on 2021-04-30 13:04

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0040_auto_20210421_1120'),
    ]

    operations = [
        migrations.CreateModel(
            name='PdfPreview',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('blob', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='+', to='data.blob')),
                ('pdf_preview', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='+', to='data.blob')),
            ],
        ),
    ]