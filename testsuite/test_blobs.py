from pathlib import Path
import pytest
from django.conf import settings
from snoop.data import models
from snoop.data import tasks

pytestmark = [pytest.mark.django_db]

def test_make_blob_from_jpeg_file():
    IMAGE = Path(settings.SNOOP_TESTDATA + "/data/disk-files/images/bikes.jpg")
    image_blob = tasks.make_blob_from_file(IMAGE)

    assert image_blob.pk == '052257179718626e83b3f8efa7fcfb42ae4dec47efab6b53c133d7415c7b62f4'
    assert image_blob.pk == image_blob.sha3_256
    assert image_blob.sha256 == '05755324b6476d2b31f2d88f1210782c3fdce880e4b6bfa9a5edb23d8be5bedb'
    assert image_blob.sha1 == '2b125736f64ff94ce423358edc5771d055cdfd7b'
    assert image_blob.md5 == '871666ee99b90e51c69af02f77f021aa'
    assert image_blob.magic == ''
    assert image_blob.mime_type == 'image/jpeg'
    assert image_blob.mime_encoding == 'binary'

