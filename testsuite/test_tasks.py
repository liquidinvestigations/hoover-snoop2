import pytest
from snoop.data.tasks import snoop_task, require_dependency, SnoopTaskBroken
from snoop.data import models

pytestmark = [pytest.mark.django_db]


def test_dependent_task(taskmanager):
    @snoop_task('test_one')
    def one():
        with models.Blob.create() as writer:
            writer.write(b'foo')

        return writer.blob

    @snoop_task('test_two')
    def two(one_result):
        return one_result

    one_task = one.laterz()
    two_task = two.laterz(depends_on={'one_result': one_task})

    taskmanager.run()

    two_task.refresh_from_db()
    with two_task.result.open() as f:
        assert f.read() == b'foo'


def test_blob_arg(taskmanager):
    @snoop_task('test_with_blob')
    def with_blob(blob, a):
        with blob.open() as src:
            data = src.read().decode('utf8')

        with models.Blob.create() as output:
            output.write(f"{data} {a}".encode('utf8'))

        return output.blob

    with models.Blob.create() as writer:
        writer.write(b'hello')

    task = with_blob.laterz(writer.blob, 'world')
    assert task.blob_arg == writer.blob

    taskmanager.run()

    task.refresh_from_db()
    with task.result.open() as f:
        assert f.read() == b'hello world'


def test_missing_dependency(taskmanager):
    @snoop_task('test_one')
    def one(message):
        with models.Blob.create() as writer:
            writer.write(message.encode('utf8'))

        return writer.blob

    @snoop_task('test_two')
    def two(**depends_on):
        return require_dependency(
            'foo', depends_on,
            lambda: one.laterz('hello'),
        )

    two_task = two.laterz()

    taskmanager.run()

    two_task.refresh_from_db()
    with two_task.result.open() as f:
        assert f.read() == b'hello'


def test_broken_dependency(taskmanager):
    @snoop_task('test_one')
    def one(message):
        raise SnoopTaskBroken(message, 'justbecause')

    @snoop_task('test_two')
    def two(**depends_on):
        try:
            require_dependency(
                'foo', depends_on,
                lambda: one.laterz('hello'),
            )

        except SnoopTaskBroken as e:
            assert 'hello' in e.args[0]
            assert e.reason == 'justbecause'

            with models.Blob.create() as writer:
                writer.write(b'it did fail')

            return writer.blob

        raise AssertionError('it did not fail!')

    two_task = two.laterz()

    taskmanager.run()

    two_task.refresh_from_db()
    with two_task.result.open() as f:
        assert f.read() == b'it did fail'
