import logging
import pytest
from fixtures import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]
PATH_IMAGE = 'disk-files/images/bikes.jpg'

logging.getLogger('exifread').setLevel(logging.INFO)


def test_digest_image_exif(client, fakedata, taskmanager):
    collection = fakedata.collection()
    with (TESTDATA / PATH_IMAGE).open('rb') as f:
        blob = fakedata.blob(f.read())
    fakedata.file(collection.root_directory, 'bikes.jpg', blob)

    taskmanager.run()

    api = CollectionApiClient(collection, client)
    digest = api.get_digest(blob.pk)['content']

    assert digest['date-created'] == '2006-02-11T11:06:37Z'
    assert digest['location'] == '33.87546081542969, -116.3016196017795'
