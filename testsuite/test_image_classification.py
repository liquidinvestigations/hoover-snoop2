import pytest
from snoop.data.analyzers import image_classification
from conftest import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]

TEST_IMAGE = TESTDATA / './disk-files/images/bikes.jpg'


def test_classification_service():
    with TEST_IMAGE.open('rb') as f:
        image_classification.call_image_classification_service(f, 'bikes.jpg')


def test_detection_service():
    with TEST_IMAGE.open('rb') as f:
        image_classification.call_object_detection_service(f, 'bikes.jpg')


def test_detection_task(fakedata):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        IMAGE_BLOB = fakedata.blob(f.read())
    fakedata.file(root, 'bike.jpg', IMAGE_BLOB)
    image_classification.detect_objects(IMAGE_BLOB)


def test_classification_task(fakedata):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        IMAGE_BLOB = fakedata.blob(f.read())
    fakedata.file(root, 'bike.jpg', IMAGE_BLOB)
    image_classification.classify_image(IMAGE_BLOB)


def test_scores_digested(fakedata, taskmanager, client):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'bikes.jpg', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']

    results_detection = digest['detected-objects']
    results_classification = digest['image-classes']

    assert [result for result in results_detection if result['object'] == 'person']
    assert [result for result in results_classification if result['class'] == 'unicycle']
