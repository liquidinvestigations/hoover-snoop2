import json
from pathlib import Path
import tempfile

from django.conf import settings
import pytest

from fixtures import TESTDATA, CollectionApiClient
from snoop.data import models
from snoop.data import tasks
from snoop.data.analyzers import email
from snoop.data.analyzers import pgp

PATH_HUSH_MAIL = 'eml-9-pgp/encrypted-hushmail-knockoff.eml'
HEIN_PRIVATE_KEY = 'eml-9-pgp/keys/hein-priv.gpg'

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def configure_gpg(monkeypatch):
    tmp = tempfile.mkdtemp()  # tempfile.TemporaryDirectory cleanup() fails

    monkeypatch.setattr(settings, 'SNOOP_GNUPG_HOME', tmp)
    path = TESTDATA / HEIN_PRIVATE_KEY
    with path.open('rb') as f:
        pgp.import_keys(f.read())

    yield


@pytest.fixture()
def gpg_blob():
    path = Path(settings.SNOOP_TESTDATA) / 'data' / PATH_HUSH_MAIL
    return models.Blob.create_from_file(path)


def test_decrypted_data(gpg_blob, configure_gpg):
    parsed_blob = email.parse(gpg_blob)

    with parsed_blob.open(encoding='utf8') as f:
        data = json.load(f)

    assert data['headers']['Subject'][0] == "Fwd: test email"
    assert data['headers']['Date'][0] == 'Wed, 10 Aug 2016 15:00:00 -0000'

    assert data['parts'][0]['pgp']
    text = data['parts'][0]['text']
    assert "This is GPG v1 speaking!" in text
    assert "Hello from the other side!" in text
    assert "Sent from my Android piece of !@#%." in text

    word_doc_pk = data['parts'][3]['attachment']['blob_pk']
    word_doc = models.Blob.objects.get(pk=word_doc_pk)
    assert word_doc.mime_type == 'application/msword'


def test_gpg_digest(gpg_blob, configure_gpg, client, fakedata, taskmanager):
    root = fakedata.init()
    fakedata.file(root, 'email', gpg_blob)

    taskmanager.run()

    api = CollectionApiClient(client)
    digest = api.get_digest(gpg_blob.pk)['content']
    assert digest['pgp']


def test_broken_if_no_gpg_home(gpg_blob):
    with pytest.raises(tasks.ShaormaBroken) as e:
        email.parse(gpg_blob)

    assert e.value.reason == 'gpg_not_configured'
