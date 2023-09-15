# Generated by Django 3.2.21 on 2023-09-15 17:22

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0056_task_partitioned_m2m_fk'),
    ]

    operations = [
        migrations.RunSQL(
            """
            INSERT INTO data_taskpartitioned ("id","func","args","date_created","date_modified","date_started","date_finished","result_id","error","status","blob_arg_id","broken_reason","log","fail_count","version")
            SELECT "id","func","args","date_created","date_modified","date_started","date_finished","result_id","error","status","blob_arg_id","broken_reason","log","fail_count","version"
            FROM data_task
            """,
            """
            INSERT INTO data_task ("id","func","args","date_created","date_modified","date_started","date_finished","result_id","error","status","blob_arg_id","broken_reason","log","fail_count","version")
            SELECT "id","func","args","date_created","date_modified","date_started","date_finished","result_id","error","status","blob_arg_id","broken_reason","log","fail_count","version"
            FROM data_taskpartitioned
            """,
        ),
    ]
