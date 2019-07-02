import sys
import json
import tempfile
import time
from pathlib import Path
import subprocess
import logging
import tarfile
from django.conf import settings
from django.core import serializers
from django.db import transaction
from django.db import connection
from django.db.utils import IntegrityError
from . import models

log = logging.getLogger(__name__)

model_map = {
    'directories': models.Directory,
    'files': models.File,
    'digests': models.Digest,
    'blobs': models.Blob,
    'tasks': models.Task,
    'task_dependencies': models.TaskDependency,
}


def build_export_queries():
    directories = models.Directory.objects.all()
    files = models.File.objects.all()
    digests = models.Digest.objects.all()
    tasks = models.Task.objects.all()
    blobs = models.Blob.objects.all()
    task_dependencies = models.TaskDependency.objects.all()

    return {
        'files': files,
        'directories': directories,
        'blobs': blobs,
        'tasks': tasks,
        'task_dependencies': task_dependencies,
        'digests': digests,
    }


@transaction.atomic
def export_db(stream=None):
    log.info("Exporting database")
    start = time.perf_counter()
    start_queries = time.perf_counter()
    queries = build_export_queries()
    log.info("Queries created in {:1.2f}s".format(time.perf_counter() - start_queries))

    if log.getEffectiveLevel() <= logging.DEBUG:
        log.debug('files: %d', len(queries['files']))
        log.debug('directories: %d', len(queries['directories']))
        log.debug('blobs: %d', len(queries['blobs']))
        log.debug('tasks: %d', len(queries['tasks']))
        log.debug('task dependencies: %d', len(queries['task_dependencies']))
        log.debug('digests: %d', len(queries['digests']))

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        json_serializer = serializers.get_serializer('json')()

        def dump(queryset, name):
            with (tmp / name).open('w', encoding='utf8') as f:
                json_serializer.serialize(queryset, stream=f, indent=2)

        start_dump = time.perf_counter()
        start_dump_sub = time.perf_counter()
        dump(queries['directories'], 'directories.json')
        log.info(" Directories dumped in {:1.2f}s".format(time.perf_counter() - start_dump_sub))
        start_dump_sub = time.perf_counter()
        dump(queries['files'], 'files.json')
        log.info(" Files dumped in {:1.2f}s".format(time.perf_counter() - start_dump_sub))
        start_dump_sub = time.perf_counter()
        dump(queries['digests'], 'digests.json')
        log.info(" Digests dumped in {:1.2f}s".format(time.perf_counter() - start_dump_sub))
        start_dump_sub = time.perf_counter()
        dump(queries['blobs'], 'blobs.json')
        log.info(" Blobs dumped in {:1.2f}s".format(time.perf_counter() - start_dump_sub))
        start_dump_sub = time.perf_counter()
        dump(queries['tasks'], 'tasks.json')
        log.info(" Tasks dumped in {:1.2f}s".format(time.perf_counter() - start_dump_sub))
        start_dump_sub = time.perf_counter()
        dump(queries['task_dependencies'], 'task_dependencies.json')
        log.info(" Task dependencies dumped at {:1.2f}s".format(time.perf_counter()
                                                                - start_dump_sub))
        log.info("Data dumped in {:1.2f}s".format(time.perf_counter() - start_dump))

        def pk_interval(queryset):
            try:
                return {
                    'min': queryset.order_by('pk')[0].pk,
                    'max': queryset.order_by('-pk')[0].pk,
                }

            except IndexError:
                return None

        serials = {
            'directories': pk_interval(queries['directories']),
            'files': pk_interval(queries['files']),
            'digests': pk_interval(queries['digests']),
            'tasks': pk_interval(queries['tasks']),
            'task_dependencies': pk_interval(queries['task_dependencies']),
        }

        with (tmp / 'serials.json').open('w', encoding='utf8') as f:
            json.dump(serials, f, indent=2)

        start_tar = time.perf_counter()
        subprocess.run(
            'tar c *',
            cwd=tmp,
            shell=True,
            check=True,
            stdout=stream,
        )
        log.info("Tar created in {:1.2f}s".format(time.perf_counter() - start_tar))

    log.info("Exporting took {:1.2f}s".format(time.perf_counter() - start))


@transaction.atomic
def import_db(stream=None):
    log.info("Importing database")
    cursor = connection.cursor()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        tar = tarfile.open(mode='r|*', fileobj=stream or sys.stdin.buffer)
        tar.extractall(tmp)
        tar.close()

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

        log.debug('deltas: %r', deltas)

        for obj in load_file('blobs.json'):
            obj.save()

        for obj in load_file('directories.json', deltas['directories']):
            if obj.parent_directory_id:
                obj.parent_directory_id += deltas['directories']
            if obj.container_file_id:
                obj.container_file_id += deltas['files']
            obj.save()

        for obj in load_file('files.json', deltas['files']):
            if obj.parent_directory_id:
                obj.parent_directory_id += deltas['directories']
            obj.save()

        for obj in load_file('digests.json', deltas['digests']):
            obj.save()

        n = 0
        duplicate_tasks = 0
        task_pk_map = {}
        for obj in load_file('tasks.json', deltas['tasks']):
            n += 1
            exists_ok = False
            if obj.func == 'archives.unarchive':
                exists_ok = True
            elif obj.func == 'digests.gather':
                pass
            elif obj.func == 'digests.index':
                pass
            elif obj.func == 'digests.launch':
                pass
            elif obj.func == 'email.msg_to_eml':
                exists_ok = True
            elif obj.func == 'email.parse':
                exists_ok = True
            elif obj.func == 'emlx.reconstruct':
                obj.args[0] += deltas['files']
            elif obj.func == 'exif.extract':
                exists_ok = True
            elif obj.func == 'filesystem.create_archive_files':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.create_attachment_files':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.handle_file':
                obj.args[0] += deltas['files']
            elif obj.func == 'filesystem.walk':
                obj.args[0] += deltas['directories']
            elif obj.func == 'tika.rmeta':
                exists_ok = True
            else:
                raise RuntimeError(f"Unexpected func {obj.func}")

            try:
                with transaction.atomic():
                    obj.save()

            except IntegrityError:
                if exists_ok:
                    existing_pk = (
                        models.Task.objects
                        .get(func=obj.func, args=obj.args)
                        .pk
                    )
                    task_pk_map[obj.pk] = existing_pk
                    duplicate_tasks += 1

                else:
                    raise

        remap_task_pk = lambda pk: task_pk_map.get(pk, pk)  # noqa: E731

        duplicate_task_dependencies = 0
        for obj in load_file('task_dependencies.json',
                             deltas['task_dependencies']):
            obj.next_id += deltas['tasks']
            obj.prev_id += deltas['tasks']
            obj.next_id = remap_task_pk(obj.next_id)
            obj.prev_id = remap_task_pk(obj.prev_id)
            try:
                with transaction.atomic():
                    obj.save()
            except IntegrityError:
                duplicate_task_dependencies += 1

        if duplicate_tasks:
            log.debug("Ignored %d duplicate tasks", duplicate_tasks)

        if duplicate_task_dependencies:
            log.debug("Ignored %d duplicate task dependencies",
                      duplicate_task_dependencies)


def export_blobs(stream=None):
    queries = build_export_queries()
    tar = tarfile.open(mode='w|', fileobj=stream or sys.stdout.buffer)

    for blob in queries['blobs'].order_by('pk'):
        log.debug('blob %r: %d', blob, blob.size)
        filename = f'{blob.pk[:2]}/{blob.pk[2:4]}/{blob.pk[4:]}'
        tarinfo = tarfile.TarInfo(filename)
        tarinfo.size = blob.size
        with blob.open() as f:
            tar.addfile(tarinfo, f)

    tar.close()


def import_blobs(stream=None):
    tar = tarfile.open(mode='r|*', fileobj=stream or sys.stdin.buffer)
    tar.extractall(settings.SNOOP_BLOB_STORAGE)
    tar.close()
