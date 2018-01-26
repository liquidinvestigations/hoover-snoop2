import json
from pathlib import Path
import pytest
from django.utils import timezone
from django.conf import settings
from snoop.data.analyzers import email
from snoop.data import models
from snoop.data import filesystem
from snoop.data import digests

pytestmark = [pytest.mark.django_db]

DATA = Path(settings.SNOOP_TESTDATA) / "data"
EML = DATA / "eml-5-long-names/Attachments have long file names..eml"
MSG = DATA / "msg-5-outlook/DISEARĂ-Te-așteptăm-la-discuția-despre-finanțarea-culturii.msg"
MAPBOX = DATA / "eml-1-promotional/Introducing Mapbox Android Services - Mapbox Team <newsletter@mapbox.com> - 2016-04-20 1603.eml"
CODINGAME = DATA / "eml-1-promotional/New on CodinGame: Check it out! - CodinGame <coders@codingame.com> - 2016-04-21 1034.eml"
CAMPUS = DATA / "eml-2-attachment/FW: Invitation Fontys Open Day 2nd of February 2014 - Campus Venlo <campusvenlo@fontys.nl> - 2013-12-16 1700.eml"
LONG_FILENAMES = DATA / "eml-5-long-names/Attachments have long file names..eml"
NO_SUBJECT = DATA / "eml-2-attachment/message-without-subject.eml"
OCTET_STREAM_CONTENT_TYPE = DATA / "eml-2-attachment/attachments-have-octet-stream-content-type.eml"
DOUBLE_DECODE_ATTACHMENT_FILENAME = DATA / "eml-8-double-encoded/double-encoding.eml"
BYTE_ORDER_MARK = DATA / "eml-bom/with-bom.eml"


@pytest.fixture(autouse=True)
def mock_collection():
    collection = models.Collection.objects.create(name='test')
    collection.directory_set.create()


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


def add_email_to_collection(path):
    [collection] = models.Collection.objects.all()
    [root] = collection.directory_set.filter(
        parent_directory__isnull=True,
        container_file__isnull=True
    ).all()
    blob = models.Blob.create_from_file(path)
    assert blob.mime_type == 'message/rfc822'
    now = timezone.now()

    return collection.file_set.create(
        parent_directory=root,
        original=blob,
        blob=blob,
        name=path.name,
        size=0,
        ctime=now,
        mtime=now,
    )


def parse_email(path):
    file = add_email_to_collection(path)
    filesystem.handle_file(file.pk)
    digests.launch(file.blob, file.collection.pk)
    [digest] = file.blob.digest_set.all()
    return digests.get_document_data(digest)


def test_subject():
    content = parse_email(MAPBOX)['content']
    assert content['subject'] == "Introducing Mapbox Android Services"


def test_no_subject_or_text():
    content = parse_email(NO_SUBJECT)['content']

    assert 'subject' in content
    assert len(content['subject']) == 0
    assert type(content['subject']) is str

    text = content['text']
    assert type(text) is str
    assert len(text) <= 2


def test_text():
    data_codin = parse_email(CODINGAME)['content']
    assert data_codin['text'].startswith("New on CodinGame: Check it out!")

    data_mapbox = parse_email(MAPBOX)['content']
    assert "Android Services includes RxJava" in data_mapbox['text']


def test_people():
    content = parse_email(MAPBOX)['content']

    assert type(content['to']) is list
    assert len(content['to']) == 1
    assert "penultim_o@yahoo.com" in content['to']

    assert type(content['from']) is str
    assert "newsletter@mapbox.com" in content['from']


def test_email_with_byte_order_mark():
    content = parse_email(BYTE_ORDER_MARK)['content']

    assert content['subject'] == "xxxxxxxxxx"
    assert content['from'] == 'yyy <yyyyyyyyyyyyyyy@gmail.com>'


def test_attachment_children():
    file = add_email_to_collection(OCTET_STREAM_CONTENT_TYPE)
    filesystem.handle_file(file.pk)

    attachments_dir = file.child_directory_set.get()
    children = attachments_dir.child_file_set.order_by('pk').all()

    assert len(children) == 3
    assert children[0].name == 'letterlegal5.doc'
    assert children[1].name == 'zip-with-pdf.zip'
    assert children[2].name == 'length.png'


def test_normal_attachments():
    data = parse_email(CAMPUS)
    children = data['children']

    assert len(children) == 2


def test_attachment_with_long_filename():
    data = parse_email(LONG_FILENAMES)
    children = data['children']

    assert len(children) == 3


def test_double_decoding_of_attachment_filenames():
    data = parse_email(DOUBLE_DECODE_ATTACHMENT_FILENAME)
    without_encoding = "atașament_pârș.jpg"
    simple_encoding = "=?utf-8?b?YXRhyJlhbWVudF9ww6JyyJkuanBn?="
    double_encoding = "=?utf-8?b?PT91dGYtOD9iP1lYUmh5S" \
                      "mxoYldWdWRGOXd3Nkp5eUprdWFuQm4/PQ==?="

    filenames = [at['filename'] for at in data.get('children')]
    assert double_encoding not in filenames
    assert {simple_encoding, without_encoding} == set(filenames)


def test_attachment_with_octet_stream_content_type():
    data = parse_email(OCTET_STREAM_CONTENT_TYPE)

    assert data['children'][0]['content_type'] == 'application/msword'
    assert data['children'][1]['content_type'] == 'application/zip'
    assert data['children'][2]['content_type'] == 'image/png'
