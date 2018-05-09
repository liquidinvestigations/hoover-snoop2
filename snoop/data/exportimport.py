import sys
import json
import tempfile
from pathlib import Path
import subprocess
from django.core import serializers
from django.db import transaction
from django.db import connection
from django.db.models.expressions import RawSQL
from django.db.models import Q
from . import models

model_map = {
    'directories': models.Directory,
    'files': models.File,
    'digests': models.Digest,
    'blobs': models.Blob,
    'tasks': models.Task,
    'task_dependencies': models.TaskDependency,
}


def info(*args):
    print(*args, file=sys.stderr)


@transaction.atomic
def export_db(collection_name, verbose=False, stream=None):
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
        info('Exporting collection', collection)
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

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        json_serializer = serializers.get_serializer('json')()

        def dump(queryset, name):
            with (tmp / name).open('w', encoding='utf8') as f:
                json_serializer.serialize(queryset, stream=f, indent=2)

        dump(directories, 'directories.json')
        dump(files, 'files.json')
        dump(digests, 'digests.json')
        dump(blobs, 'blobs.json')
        dump(tasks, 'tasks.json')
        dump(task_dependencies, 'task_dependencies.json')

        def pk_interval(queryset):
            try:
                return {
                    'min': queryset.order_by('pk')[0].pk,
                    'max': queryset.order_by('-pk')[0].pk,
                }

            except IndexError:
                return None

        serials = {
            'directories': pk_interval(directories),
            'files': pk_interval(files),
            'digests': pk_interval(digests),
            'tasks': pk_interval(tasks),
            'task_dependencies': pk_interval(task_dependencies),
        }

        with (tmp / 'serials.json').open('w', encoding='utf8') as f:
            json.dump(serials, f, indent=2)

        subprocess.run(
            'tar c *',
            cwd=tmp,
            shell=True,
            check=True,
            stdout=stream,
        )


@transaction.atomic
def import_db(collection_name, verbose=False, stream=None):
    if verbose:
        info("Importing collection", collection_name)

    cursor = connection.cursor()
    collection = models.Collection.objects.create(name=collection_name,
                                                  root='')

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        subprocess.run(
            'tar x',
            cwd=tmp,
            shell=True,
            check=True,
            stdin=stream,
        )

        with (tmp / 'serials.json').open(encoding='utf8') as f:
            serials = json.load(f)

        def adjust_seq(name):
            if not serials[name]:
                return 0
            table = model_map[name]._meta.db_table
            id_seq = f'{table}_id_seq'
            cursor.execute(f"LOCK TABLE {table}")
            cursor.execute(f"SELECT last_value FROM {id_seq}")
            current_value = list(cursor)[0][0]
            delta = current_value - serials[name]['min'] + 1
            interval_length = serials[name]['max'] - serials[name]['min'] + 1
            next_value = current_value + interval_length
            cursor.execute(f"SELECT setval('{id_seq}', {next_value})")
            return delta

        def load_file(name, pk_delta=None):
            with (tmp / name).open(encoding='utf8') as f:
                for record in serializers.deserialize('json', f):
                    obj = record.object
                    if pk_delta:
                        obj.pk += pk_delta
                    yield obj

        deltas = {
            name: adjust_seq(name)
            for name in model_map
            if name != 'blobs'
        }

        if verbose:
            info('deltas:', deltas)

        for obj in load_file('blobs.json'):
            obj.save()

        for obj in load_file('directories.json', deltas['directories']):
            obj.collection = collection
            if obj.parent_directory_id:
                obj.parent_directory_id += deltas['directories']
            if obj.container_file_id:
                obj.container_file_id += deltas['files']
            obj.save()

        for obj in load_file('files.json', deltas['files']):
            obj.collection = collection
            if obj.parent_directory_id:
                obj.parent_directory_id += deltas['directories']
            obj.save()

        for obj in load_file('digests.json', deltas['digests']):
            obj.collection = collection
            obj.save()

        n = 0
        for obj in load_file('tasks.json', deltas['tasks']):
            n += 1
            if obj.func == 'archives.unarchive':
                pass
            elif obj.func == 'digests.gather':
                obj.args[1] = collection.pk
            elif obj.func == 'digests.index':
                obj.args[1] = collection.pk
            elif obj.func == 'digests.launch':
                obj.args[1] = collection.pk
            elif obj.func == 'email.msg_to_eml':
                pass
            elif obj.func == 'email.parse':
                pass
            elif obj.func == 'emlx.reconstruct':
                obj.args[0] += deltas['files']
            elif obj.func == 'exif.extract':
                pass
            elif obj.func == 'filesystem.create_archive_files':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.create_attachment_files':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.handle_file':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.walk':
                obj.args[0] += deltas['directories']
            elif obj.func == 'tika.rmeta':
                pass
            else:
                raise RuntimeError(f"Unexpected func {obj.func}")

            obj.save()

        for obj in load_file('task_dependencies.json',
                             deltas['task_dependencies']):
            obj.next_id += deltas['tasks']
            obj.prev_id += deltas['tasks']
            obj.save()
