import pytest

from conftest import CollectionApiClient

pytestmark = [pytest.mark.django_db]


def test_blob_locations(client, fakedata, taskmanager):
    root_directory = fakedata.init()
    dir1 = fakedata.directory(root_directory, 'dir1')
    dir2 = fakedata.directory(root_directory, 'dir2')
    blob = fakedata.blob(b'hello world')
    fakedata.file(dir1, 'foo', blob)
    fakedata.file(dir2, 'bar', blob)

    taskmanager.run()

    def directory_id(directory):
        return f'_directory_{directory.pk}'

    api = CollectionApiClient(client)
    resp = api.get_locations(blob.pk)
    assert resp['locations'] == [
        {
            'filename': 'foo',
            'parent_id': directory_id(dir1),
            'parent_path': '/dir1',
        },
        {
            'filename': 'bar',
            'parent_id': directory_id(dir2),
            'parent_path': '/dir2',
        },
    ]
