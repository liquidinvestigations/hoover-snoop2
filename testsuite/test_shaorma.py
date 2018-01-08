import pytest
from snoop.data.tasks import shaorma
from snoop.data import models

pytestmark = [pytest.mark.django_db]


def test_dependent_task():
    @shaorma
    def one():
        with models.Blob.create() as writer:
            writer.write(b'foo')

        return writer.blob

    @shaorma
    def two(one_result):
        return one_result

    one_task = one.laterz()
    two_task = two.laterz(depends_on={'one_result': one_task})
    two_task.refresh_from_db()
    with two_task.result.open() as f:
        assert f.read() == b'foo'
