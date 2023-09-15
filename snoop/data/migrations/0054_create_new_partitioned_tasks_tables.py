# Generated by Django 3.2.21 on 2023-09-15 14:34

from django.db import migrations, models
import django.db.models.deletion
import psqlextra.backend.migrations.operations.create_partitioned_model
import psqlextra.indexes.unique_index
import psqlextra.manager.manager
import psqlextra.models.partitioned
import psqlextra.types


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0053_digest_data_digest_date_cr_fb2d24_idx'),
    ]

    operations = [
        migrations.AlterField(
            model_name='task',
            name='blob_arg',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='task_arg_set_old', to='data.blob'),
        ),
        migrations.AlterField(
            model_name='taskdependency',
            name='next',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='prev_set_old', to='data.task'),
        ),
        migrations.AlterField(
            model_name='taskdependency',
            name='prev',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='next_set_old', to='data.task'),
        ),
        psqlextra.backend.migrations.operations.create_partitioned_model.PostgresCreatePartitionedModel(
            name='TaskPartitioned',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('func', models.CharField(max_length=1024)),
                ('args', models.JSONField()),
                ('date_created', models.DateTimeField(auto_now_add=True)),
                ('date_modified', models.DateTimeField(auto_now=True)),
                ('date_started', models.DateTimeField(blank=True, null=True)),
                ('date_finished', models.DateTimeField(blank=True, null=True)),
                ('version', models.IntegerField(default=0)),
                ('fail_count', models.IntegerField(default=0)),
                ('status', models.CharField(default='pending', max_length=16)),
                ('error', models.TextField(blank=True)),
                ('broken_reason', models.CharField(blank=True, default='', max_length=128)),
                ('log', models.TextField(blank=True)),
                ('blob_arg', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='task_arg_set', to='data.blob')),
                ('result', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.RESTRICT, to='data.blob')),
            ],
            partitioning_options={
                'method': psqlextra.types.PostgresPartitioningMethod['HASH'],
                'key': ['func', 'args'],
            },
            bases=(psqlextra.models.partitioned.PostgresPartitionedModel,),
            managers=[
                ('objects', psqlextra.manager.manager.PostgresManager()),
            ],
        ),
        migrations.CreateModel(
            name='TaskDependencyPartitioned',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('prev_func', models.CharField(max_length=1024)),
                ('prev_args', models.JSONField()),
                ('next_func', models.CharField(max_length=1024)),
                ('next_args', models.JSONField()),
                ('name', models.CharField(max_length=1024)),
            ],
            options={
                'verbose_name_plural': 'task dependencies_p',
                'unique_together': {('prev_func', 'next_func', 'prev_args', 'next_args', 'name')},
            },
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=psqlextra.indexes.unique_index.UniqueIndex(fields=['func', 'args'], name='data_taskpa_func_0609f2_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['status'], name='data_taskpa_status_ad7925_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['date_finished'], name='data_taskpa_date_fi_cee01f_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['func', 'status'], name='data_taskpa_func_2ed6ba_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['status', 'date_modified'], name='data_taskpa_status_e9181f_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['func', 'date_modified'], name='data_taskpa_func_f10cf0_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['broken_reason'], name='data_taskpa_broken__5a76f6_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['func', 'date_started', 'date_finished'], name='data_taskpa_func_b62ce4_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['func', 'version'], name='data_taskpa_func_f72a50_idx'),
        ),
        migrations.AddIndex(
            model_name='taskpartitioned',
            index=models.Index(fields=['status', 'fail_count'], name='data_taskpa_status_e0ae3a_idx'),
        ),
    ]
