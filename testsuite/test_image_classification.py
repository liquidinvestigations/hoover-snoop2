import json

import pytest
from snoop.data.analyzers import image_classification

from conftest import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]

TEST_IMAGE = TESTDATA / './disk-files/images/bikes.jpg'

EXPECTED_OBJECTS = ['person', 'bicycle']
EXPECTED_CLASSE = 'unicycle'


def test_classification_service_endpoint():
    with TEST_IMAGE.open('rb') as f:
        image_classification.call_image_classification_service(f, 'bikes.jpg')


def test_detection_service_endpoint():
    with TEST_IMAGE.open('rb') as f:
        image_classification.call_object_detection_service(f, 'bikes.jpg')


def test_classification_service():
    with TEST_IMAGE.open('rb') as f:
        predictions = image_classification.call_image_classification_service(f, 'bikes.jpg')
    classes = [hit[0] for hit in predictions]
    assert EXPECTED_CLASS in classes


def test_detection_service():
    with TEST_IMAGE.open('rb') as f:
        predictions = image_classification.call_object_detection_service(f, 'bikes.jpg')
    objects = [hit['name'] for hit in predictions]
    assert all(hit in objects for hit in EXPECTED_OBJECTS)


def test_detection_task(fakedata):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        IMAGE_BLOB = fakedata.blob(f.read())
    fakedata.file(root, 'bike.jpg', IMAGE_BLOB)
    with image_classification.detect_objects(IMAGE_BLOB).open() as f:
        results = json.load(f)
    objects = [hit['object'] for hit in results]
    assert all(hit in objects for hit in EXPECTED_OBJECTS)


def test_classification_task(fakedata):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        IMAGE_BLOB = fakedata.blob(f.read())
    fakedata.file(root, 'bike.jpg', IMAGE_BLOB)
    with image_classification.classify_image(IMAGE_BLOB).open() as f:
        results = json.load(f)
    classes = [hit['class'] for hit in results]
    assert EXPECTED_CLASS in classes


def test_scores_digested(fakedata, taskmanager, client):
    root = fakedata.init()
    with TEST_IMAGE.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'bikes.jpg', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']

    assert [result for result in digest['detected-objects'] if result['object'] == 'person']
    assert [result for result in digest['image-classes'] if result['class'] == 'unicycle']
