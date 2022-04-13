import pytest
from snoop.data import models
from snoop.data import collections
from snoop.data.analyzers import entities

from conftest import TESTDATA, CollectionApiClient

pytestmark = [pytest.mark.django_db]


TEST_TEXT = 'Hello, I am Barack Obama and I live in Washington DC'


def test_nlp_service():
    resp = entities.call_nlp_server('entity_extraction', {'text': TEST_TEXT})
    entity = resp['entities'][0]
    assert entity['text'] == 'Barack Obama' and entity['type'] == 'PER'


def test_extract_entities(fakedata, taskmanager, client):
    root = fakedata.init()
    test_doc = TESTDATA / './disk-files/pdf-doc-txt/easychair.odt'
    with test_doc.open('rb') as f:
        blob = fakedata.blob(f.read())

    fakedata.file(root, 'file.odt', blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(blob.pk)['content']

    print(collections.current())
    print(models.Task.objects.all())
    print(models.Entity.objects.all())
    print(api.get_digest(blob.pk))

    assert models.Entity.objects.filter(entity='Andrei Voronkov').exists()

    assert 'Microsoft' in digest['entity-type.organization']
    assert 'Microsoft' in digest['entity']
    assert 'Manchester' in digest['entity-type.location']
