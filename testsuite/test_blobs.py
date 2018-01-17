from pathlib import Path
import pytest
from django.conf import settings
from snoop.data import models
from snoop.data import tasks

pytestmark = [pytest.mark.django_db]

def test_make_blob_from_jpeg_file():
    IMAGE = settings.SNOOP_TESTDATA + "/data/disk-files/images/bikes.jpg"
    image_blob = models.Blob.create_from_file(IMAGE)

    assert image_blob.pk == '052257179718626e83b3f8efa7fcfb42ae4dec47efab6b53c133d7415c7b62f4'
    assert image_blob.pk == image_blob.sha3_256
    assert image_blob.sha256 == '05755324b6476d2b31f2d88f1210782c3fdce880e4b6bfa9a5edb23d8be5bedb'
    assert image_blob.sha1 == '2b125736f64ff94ce423358edc5771d055cdfd7b'
    assert image_blob.md5 == '871666ee99b90e51c69af02f77f021aa'
    assert 'JPEG image data' in image_blob.magic
    assert image_blob.mime_type == 'image/jpeg'
    assert image_blob.mime_encoding == 'binary'

def test_make_blob_from_first_eml_file():
    EML = settings.SNOOP_TESTDATA + "/data/eml-8-double-encoded/simple-encoding.eml"
    eml_blob = models.Blob.create_from_file(EML)

    assert eml_blob.sha256 == '173eb1bc20865d3a9d2b4ac91484b06b59fdea8bc25f6e18fdf837de1f6a80e9'
    assert eml_blob.mime_type == 'message/rfc822'
    assert eml_blob.mime_encoding == 'us-ascii'

def test_make_blob_from_a_partial_emlx_file():
    EMLX = settings.SNOOP_TESTDATA + "/data/lists.mbox/F2D0D67E-7B19-4C30-B2E9-B58FE4789D51/Data/1/Messages/1498.partial.emlx"
    emlx_blob = models.Blob.create_from_file(EMLX)

    assert emlx_blob.sha256 == '1199ba0bab414a740428f604f08e4d2e1f8366a8bcc16478f82ab0e890d5de09'
    assert emlx_blob.mime_type == 'message/x-emlx'
    assert emlx_blob.mime_encoding == 'utf-8'
