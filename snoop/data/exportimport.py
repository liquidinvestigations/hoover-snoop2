import sys
from django.db import transaction
from django.db.models.expressions import RawSQL
from django.db.models import Q
from . import models


@transaction.atomic
def export_db(collection_name, verbose=False):
    collection = models.Collection.objects.get(name=collection_name)

    directories = collection.directory_set.all()
    files = collection.file_set.all()
    digests = collection.digest_set.all()

    file_original_pks = (
        collection.file_set
        .values_list('original_id', flat=True)
    )

    file_blob_pks = (
        collection.file_set
        .values_list('blob_id', flat=True)
    )

    archives_unarchive_tasks = (
        models.Task.objects
        .filter(func='archives.unarchive')
        .filter(blob_arg__in=file_blob_pks)
    )

    digests_gather_tasks = (
        models.Task.objects
        .filter(func='digests.gather')
        .annotate(arg1=RawSQL('(args ->> 1)::integer', ()))
        .filter(arg1=collection.pk)
    )

    digests_index_tasks = (
        models.Task.objects
        .filter(func='digests.index')
        .annotate(arg1=RawSQL('(args ->> 1)::integer', ()))
        .filter(arg1=collection.pk)
    )

    digests_launch_tasks = (
        models.Task.objects
        .filter(func='digests.launch')
        .annotate(arg1=RawSQL('(args ->> 1)::integer', ()))
        .filter(arg1=collection.pk)
    )

    email_msg_to_eml_tasks = (
        models.Task.objects
        .filter(func='email.msg_to_eml')
        .filter(blob_arg__in=file_original_pks)
    )

    email_parse_tasks = (
        models.Task.objects
        .filter(func='email.parse')
        .filter(blob_arg__in=file_blob_pks)
    )

    emlx_reconstruct_tasks = (
        models.Task.objects
        .filter(func='emlx.reconstruct')
        .annotate(arg0=RawSQL('(args ->> 0)::integer', ()))
        .filter(arg0__in=files)
    )

    exif_extract_tasks = (
        models.Task.objects
        .filter(func='exif.extract')
        .filter(blob_arg__in=file_blob_pks)
    )

    filesystem_create_archive_files_tasks = (
        models.Task.objects
        .filter(func='filesystem.create_archive_files')
        .annotate(arg0=RawSQL('(args ->> 0)::integer', ()))
        .filter(arg0__in=files)
    )

    filesystem_create_attachment_files_tasks = (
        models.Task.objects
        .filter(func='filesystem.create_attachment_files')
        .annotate(arg0=RawSQL('(args ->> 0)::integer', ()))
        .filter(arg0__in=files)
    )

    filesystem_handle_file_tasks = (
        models.Task.objects
        .filter(func='filesystem.handle_file')
        .annotate(arg0=RawSQL('(args ->> 0)::integer', ()))
        .filter(arg0__in=files)
    )

    filesystem_walk_tasks = (
        models.Task.objects
        .filter(func='filesystem.walk')
        .annotate(arg0=RawSQL('(args ->> 0)::integer', ()))
        .filter(arg0__in=directories)
    )

    tika_rmeta_email_tasks = (
        models.Task.objects.filter(next_set__in=(
            models.TaskDependency.objects
            .filter(next__in=email_parse_tasks)
        ))
        .filter(func='tika.rmeta')
    )
    tika_rmeta_file_tasks = (
        models.Task.objects
        .filter(func='tika.rmeta')
        .filter(blob_arg__in=file_blob_pks)
    )
    tika_rmeta_tasks = set()
    tika_rmeta_tasks.update(tika_rmeta_email_tasks)
    tika_rmeta_tasks.update(tika_rmeta_file_tasks)

    tasks = (
        models.Task.objects
        .filter(
            Q(pk__in=archives_unarchive_tasks) |
            Q(pk__in=digests_gather_tasks) |
            Q(pk__in=digests_index_tasks) |
            Q(pk__in=digests_launch_tasks) |
            Q(pk__in=email_msg_to_eml_tasks) |
            Q(pk__in=email_parse_tasks) |
            Q(pk__in=emlx_reconstruct_tasks) |
            Q(pk__in=exif_extract_tasks) |
            Q(pk__in=filesystem_create_archive_files_tasks) |
            Q(pk__in=filesystem_create_attachment_files_tasks) |
            Q(pk__in=filesystem_handle_file_tasks) |
            Q(pk__in=filesystem_walk_tasks) |
            Q(pk__in=tika_rmeta_email_tasks) |
            Q(pk__in=tika_rmeta_file_tasks)
        )
    )

    blobs = (
        models.Blob.objects
        .filter(
            Q(pk__in=file_original_pks) |
            Q(pk__in=file_blob_pks) |
            Q(pk__in=tasks.values_list('result_id', flat=True)) |
            Q(pk__in=tasks.values_list('blob_arg_id', flat=True))
        )
    )

    task_dependencies = (
        models.TaskDependency.objects
        .filter(next__in=tasks)
    )

    if verbose:
        def info(*args):
            print(*args, file=sys.stderr)

        info('task archives.unarchive:', len(archives_unarchive_tasks))
        info('task digests.gather:', len(digests_gather_tasks))
        info('task digests.index:', len(digests_index_tasks))
        info('task digests.launch:', len(digests_launch_tasks))
        info('task email.msg_to_eml:', len(email_msg_to_eml_tasks))
        info('task email.parse:', len(email_parse_tasks))
        info('task emlx.reconstruct:', len(emlx_reconstruct_tasks))
        info('task exif.extract:', len(exif_extract_tasks))
        info('task filesystem.create_archive_files:',
             len(filesystem_create_archive_files_tasks))
        info('task filesystem.create_attachment_files:',
             len(filesystem_create_attachment_files_tasks))
        info('task filesystem.handle_file:', len(filesystem_handle_file_tasks))
        info('task filesystem.walk:', len(filesystem_walk_tasks))
        info('task tika.rmeta:', len(tika_rmeta_tasks))
        info('files:', len(files))
        info('directories:', len(directories))
        info('blobs:', len(blobs))
        info('tasks:', len(tasks))
        info('task dependencies:', len(task_dependencies))
        info('digests:', len(digests))
