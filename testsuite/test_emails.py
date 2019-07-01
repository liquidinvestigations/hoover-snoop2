import json
from pathlib import Path
import pytest
from django.utils import timezone
from django.conf import settings
from snoop.data.analyzers import email
from snoop.data.analyzers import emlx
from snoop.data import models
from snoop.data import filesystem
from snoop.data import digests
from conftest import mkdir, mkfile

pytestmark = [pytest.mark.django_db]

DATA = Path(settings.SNOOP_TESTDATA) / "data"
EML = DATA / "eml-5-long-names/Attachments have long file names..eml"
MSG = DATA / "msg-5-outlook/DISEARĂ-Te-așteptăm-la-discuția-despre-finanțarea-culturii.msg"
MAPBOX = DATA / ("eml-1-promotional/Introducing Mapbox Android Services - Mapbox Team "
                 "<newsletter@mapbox.com> - 2016-04-20 1603.eml")
CODINGAME = DATA / ("eml-1-promotional/New on CodinGame: Check it out! - CodinGame "
                    "<coders@codingame.com> - 2016-04-21 1034.eml")
CAMPUS = DATA / ("eml-2-attachment/FW: Invitation Fontys Open Day 2nd of February 2014 - Campus "
                 "Venlo <campusvenlo@fontys.nl> - 2013-12-16 1700.eml")
LONG_FILENAMES = DATA / "eml-5-long-names/Attachments have long file names..eml"
NO_SUBJECT = DATA / "eml-2-attachment/message-without-subject.eml"
OCTET_STREAM_CONTENT_TYPE = DATA / "eml-2-attachment/attachments-have-octet-stream-content-type.eml"
DOUBLE_DECODE_ATTACHMENT_FILENAME = DATA / "eml-8-double-encoded/double-encoding.eml"
BYTE_ORDER_MARK = DATA / "eml-bom/with-bom.eml"


@pytest.fixture(autouse=True)
def root_directory():
    models.Directory.objects.create()


def test_convert_msg_to_eml():
    msg_blob = models.Blob.create_from_file(MSG)
    eml_blob = email.msg_to_eml(msg_blob)

    assert eml_blob.mime_type == 'message/rfc822'


def test_email_header_parsing():
    eml_blob = models.Blob.create_from_file(EML)
    eml_data_blob = email.parse(eml_blob)
    with eml_data_blob.open() as f:
        content = json.load(f)
    assert content['headers']['Subject'] == ['Attachments have long file names.']


def add_email_to_collection(path):
    blob = models.Blob.create_from_file(path)
    assert blob.mime_type == 'message/rfc822'
    now = timezone.now()

    return models.File.objects.create(
        parent_directory=models.Directory.root(),
        original=blob,
        blob=blob,
        name_bytes=path.name.encode('utf8'),
        size=0,
        ctime=now,
        mtime=now,
    )


def parse_email(path, taskmanager):
    file = add_email_to_collection(path)
    filesystem.handle_file(file.pk)
    digests.launch(file.blob)
    taskmanager.run()
    return digests.get_document_data(file.blob.digest)


def test_subject_and_date(taskmanager):
    content = parse_email(MAPBOX, taskmanager)['content']
    assert content['subject'] == "Introducing Mapbox Android Services"
    assert content['date'] == '2016-04-20T13:03:20Z'


def test_no_subject_or_text(taskmanager):
    content = parse_email(NO_SUBJECT, taskmanager)['content']

    assert 'subject' in content
    assert len(content['subject']) == 0
    assert type(content['subject']) is str

    text = content['text']
    assert type(text) is str
    assert len(text) <= 2


def test_text(taskmanager):
    data_codin = parse_email(CODINGAME, taskmanager)['content']
    assert data_codin['text'].startswith("New on CodinGame: Check it out!")

    data_mapbox = parse_email(MAPBOX, taskmanager)['content']
    assert "Android Services includes RxJava" in data_mapbox['text']


def test_people(taskmanager):
    content = parse_email(MAPBOX, taskmanager)['content']

    assert type(content['to']) is list
    assert len(content['to']) == 1
    assert "penultim_o@yahoo.com" in content['to']

    assert type(content['from']) is str
    assert "newsletter@mapbox.com" in content['from']

    assert ['yahoo.com', 'mapbox.com'] == content['email-domains']


def test_email_with_byte_order_mark(taskmanager):
    content = parse_email(BYTE_ORDER_MARK, taskmanager)['content']

    assert content['subject'] == "xxxxxxxxxx"
    assert content['from'] == 'yyy <yyyyyyyyyyyyyyy@gmail.com>'


def test_attachment_children(taskmanager):
    file = add_email_to_collection(OCTET_STREAM_CONTENT_TYPE)
    filesystem.handle_file(file.pk)
    taskmanager.run()

    attachments_dir = file.child_directory_set.get()
    children = attachments_dir.child_file_set.order_by('pk').all()

    assert len(children) == 3
    assert children[0].name == 'letterlegal5.doc'
    assert children[1].name == 'zip-with-pdf.zip'
    assert children[2].name == 'length.png'


def test_normal_attachments(taskmanager):
    data = parse_email(CAMPUS, taskmanager)
    children = data['children']

    assert len(children) == 2


def test_attachment_with_long_filename(taskmanager):
    data = parse_email(LONG_FILENAMES, taskmanager)
    children = data['children']

    assert len(children) == 3


def test_double_decoding_of_attachment_filenames(taskmanager):
    data = parse_email(DOUBLE_DECODE_ATTACHMENT_FILENAME, taskmanager)
    without_encoding = "atașament_pârș.jpg"
    simple_encoding = "=?utf-8?b?YXRhyJlhbWVudF9ww6JyyJkuanBn?="
    double_encoding = "=?utf-8?b?PT91dGYtOD9iP1lYUmh5S" \
                      "mxoYldWdWRGOXd3Nkp5eUprdWFuQm4/PQ==?="

    filenames = [at['filename'] for at in data.get('children')]
    assert double_encoding not in filenames
    assert {simple_encoding, without_encoding} == set(filenames)


def test_attachment_with_octet_stream_content_type(taskmanager):
    data = parse_email(OCTET_STREAM_CONTENT_TYPE, taskmanager)

    assert data['children'][0]['content_type'] == 'image/png'
    assert data['children'][1]['content_type'] == 'application/msword'
    assert data['children'][2]['content_type'] == 'application/zip'


def test_broken_header():
    eml = DATA / 'eml-10-broken-header/broken-subject.eml'
    blob = models.Blob.create_from_file(eml)
    result = email.parse(blob)
    with result.open(encoding='utf8') as f:
        data = json.load(f)
    assert data['headers']['Subject'] == [(
        "A\ufffd\ufffda crap\ufffd\ufffd "
        "headerul fle\ufffd\ufffdc\ufffd\ufffdit"
    )]


def test_emlx_reconstruction(taskmanager):
    root = mkdir(None, '')
    d1 = mkdir(root, 'lists.mbox')
    d2 = mkdir(d1, 'F2D0D67E-7B19-4C30-B2E9-B58FE4789D51')
    d3 = mkdir(d2, 'Data')
    d4 = mkdir(d3, '1')
    d5 = mkdir(d4, 'Messages')

    emlx_filename = '1498.partial.emlx'
    emlx_path = (
        Path(settings.SNOOP_TESTDATA) / 'data'
        / d1.name / d2.name / d3.name
        / d4.name / d5.name / emlx_filename
    )
    emlx_blob = models.Blob.create_from_file(emlx_path)
    emlx_file = mkfile(d5, emlx_filename, emlx_blob)

    emlxpart_filename = '1498.3.emlxpart'
    emlxpart_path = (
        Path(settings.SNOOP_TESTDATA) / 'data'
        / d1.name / d2.name / d3.name
        / d4.name / d5.name / emlxpart_filename
    )
    emlxpart_blob = models.Blob.create_from_file(emlxpart_path)
    mkfile(d5, emlxpart_filename, emlxpart_blob)

    emlx_task = emlx.reconstruct.laterz(emlx_file.pk)
    taskmanager.run()
    emlx_task.refresh_from_db()
    emlx_file.blob = emlx_task.result
    emlx_file.save()

    eml_task = email.parse.laterz(emlx_file.blob)
    taskmanager.run()
    eml_task.refresh_from_db()

    attachments = list(filesystem.get_email_attachments(eml_task.result))

    size = {
        a['name']: models.Blob.objects.get(pk=a['blob_pk']).size
        for a in attachments
    }

    assert size['Legea-299-2015-informatiile-publice.odt'] == 28195
    assert size['Legea-299-2015-informatiile-publice.pdf'] == 55904


def test_emlx_reconstruction_with_missing_file(taskmanager):
    root = mkdir(None, '')
    d1 = mkdir(root, 'emlx-4-missing-part')
    emlx_filename = '1498.partial.emlx'
    emlx_path = Path(settings.SNOOP_TESTDATA) / 'data' / d1.name / emlx_filename
    emlx_blob = models.Blob.create_from_file(emlx_path)
    emlx_file = mkfile(d1, emlx_filename, emlx_blob)

    emlx_task = emlx.reconstruct.laterz(emlx_file.pk)
    taskmanager.run()
    emlx_task.refresh_from_db()
    emlx_file.blob = emlx_task.result
    emlx_file.save()

    eml_task = email.parse.laterz(emlx_file.blob)
    taskmanager.run()
    eml_task.refresh_from_db()

    attachments = list(filesystem.get_email_attachments(eml_task.result))

    size = {
        a['name']: models.Blob.objects.get(pk=a['blob_pk']).size
        for a in attachments
    }

    assert size['Legea-299-2015-informatiile-publice.odt'] == 28195
    assert size['Legea-299-2015-informatiile-publice.pdf'] == 0
