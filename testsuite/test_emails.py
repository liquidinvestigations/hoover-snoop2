import json
from pathlib import Path
import pytest
from snoop.data.analyzers import email
from django.conf import settings
from snoop.data import models

pytestmark = [pytest.mark.django_db]

EML = Path(settings.SNOOP_TESTDATA) / "data/eml-5-long-names/Attachments have long file names..eml"
MSG = Path(settings.SNOOP_TESTDATA) / "data/msg-5-outlook/DISEARĂ-Te-așteptăm-la-discuția-despre-finanțarea-culturii.msg"

def test_convert_msg_to_eml():
    msg_blob = models.Blob.create_from_file(MSG)
    eml_blob = email.msg_blob_to_eml(msg_blob)

    assert eml_blob.mime_type == 'message/rfc822'

def test_email_header_parsing():
    eml_blob = models.Blob.create_from_file(EML)
    eml_data_blob = email.parse(eml_blob)
    with eml_data_blob.open() as f:
        content = json.load(f)
    assert content['headers']['Subject'] == ['Attachments have long file names.']
