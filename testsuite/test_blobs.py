import pytest
from django.conf import settings
from snoop.data import models

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


@pytest.mark.parametrize('testdata_relative_path, expected_mime_type', [
    # .eml: message/rfc822
    ("/data/no-extension/file_eml", 'message/rfc822'),
    ("/data/eml-2-attachment/message-without-subject.eml", 'message/rfc822'),
    ("/data/eml-2-attachment/Fwd: The American College of Thessaloniki - Greece - Tarek Kouatly <tarek@act.edu> - 2013-11-11 1622.eml", 'message/rfc822'),
    ("/data/eml-2-attachment/attachments-have-octet-stream-content-type.eml", 'message/rfc822'),
    ("/data/eml-2-attachment/FW: Invitation Fontys Open Day 2nd of February 2014 - Campus Venlo <campusvenlo@fontys.nl> - 2013-12-16 1700.eml", 'message/rfc822'),
    ("/data/eml-2-attachment/Urăsc canicula, e nașpa.eml", 'message/rfc822'),
    ("/data/eml-5-long-names/Attachments have long file names..eml", 'message/rfc822'),
    ("/data/eml-bom/with-bom.eml", 'message/rfc822'),
    ("/data/eml-1-promotional/Introducing Mapbox Android Services - Mapbox Team <newsletter@mapbox.com> - 2016-04-20 1603.eml", 'message/rfc822'),
    ("/data/eml-1-promotional/Machine Learning comes to CodinGame! - CodinGame Team <contact@codingame.com> - 2016-04-22 1731.eml", 'message/rfc822'),
    ("/data/eml-1-promotional/New on CodinGame: Check it out! - CodinGame <coders@codingame.com> - 2016-04-21 1034.eml", 'message/rfc822'),
    ("/data/eml-8-double-encoded/simple-encoding.eml", 'message/rfc822'),
    ("/data/eml-8-double-encoded/double-encoding.eml", 'message/rfc822'),
    ("/data/eml-3-uppercaseheaders/Fwd: The American College of Thessaloniki - Greece - Tarek Kouatly <tarek@act.edu> - 2013-11-11 1622.eml", 'message/rfc822'),
    ("/data/eml-9-pgp/encrypted-hushmail-knockoff.eml", 'message/rfc822'),
    ("/data/eml-9-pgp/encrypted-machine-learning-comes.eml", 'message/rfc822'),
    ("/data/eml-9-pgp/encrypted-hushmail-smashed-bytes.eml", 'message/rfc822'),

    # text/plain
    ("/data/disk-files/long-filenames/Sample text file with a long filename Sample text file with a long filename Sample text file with a long filename .txt", 'text/plain'),
    ("/data/disk-files/pdf-doc-txt/easychair.txt", 'text/plain'),
    ("/data/words/usr-share-dict-words.txt", 'text/plain'),

    # .emlx: message/x-emlx
    ("/data/lists.mbox/F2D0D67E-7B19-4C30-B2E9-B58FE4789D51/Data/1/Messages/1498.partial.emlx", 'message/x-emlx'),

    # .mbox: application/mbox
    ("/data/mbox/2018-March.txt", "application/mbox"),
    ("/data/mbox/shapelib.mbox", "application/mbox"),

    # .pdf
    ("/ocr/one/foo/bar/f/d/fd41b8f1fe19c151517b3cda2a615fa8.pdf", "application/pdf"),
    ("/data/no-extension/file_pdf", "application/pdf"),
    ("/data/disk-files/pdf-for-ocr/mof1_1992_233.pdf", "application/pdf"),
    ("/data/disk-files/long-filenames/THIS_FILENAME_IS_LONGER_THAN_ANY_DECENT_MARGIN_AND_ITS_ONLY_ONE_WORD_WITHOUT_ANY_BREAKS_WHATSOEVER_THIS_IS_GETTING_BAD.pdf", "application/pdf"),
    ("/data/disk-files/pdf-doc-txt/stanley.ec02.pdf", "application/pdf"),
    ("/data/disk-files/duplicates/one.pdf", "application/pdf"),
    ("/data/disk-files/duplicates/three.pdf", "application/pdf"),
    ("/data/disk-files/duplicates/two.pdf", "application/pdf"),
    ("/data/disk-files/duplicates/some/other/deep/path/five.pdf", "application/pdf"),
    ("/data/lists.mbox/F2D0D67E-7B19-4C30-B2E9-B58FE4789D51/Data/1/Attachments/1498/3/Legea-299-2015-informatiile-publice.pdf", "application/pdf"),
    ("/data/eml-9-pgp/stuff/cleartext/adobe-acrobat-xi-create-form-or-template-tutorial_ue.pdf", "application/pdf"),

    # .doc
    ("/data/no-extension/file_doc", "application/msword"),
    ("/data/disk-files/long-filenames/This filename is just really long long long long long long long long long long long long long long long long.doc", "application/msword"),
    ("/data/disk-files/pdf-doc-txt/sample (1).doc", "application/msword"),
    ("/data/eml-9-pgp/stuff/cleartext/wd-spectools-word-sample-04.doc", "application/msword"),
    # .odt
    ("/data/no-extension/file_odt", "application/vnd.oasis.opendocument.text"),
    ("/data/disk-files/pdf-doc-txt/easychair.odt", "application/vnd.oasis.opendocument.text"),
    ("/data/lists.mbox/F2D0D67E-7B19-4C30-B2E9-B58FE4789D51/Data/1/Attachments/1498/2/Legea-299-2015-informatiile-publice.odt", "application/vnd.oasis.opendocument.text"),
    # .html
    ("/data/no-extension/file_html", "text/html"),
    ("/data/disk-files/bad-html/alert.html", "text/html"),
    ("/data/disk-files/html-encodings/meta_encoding_latin1.html", "text/html"),
    ("/data/disk-files/html-encodings/xml_declaration_latin1.html", "text/xml"),
    # .docx
    ("/data/no-extension/file_docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ("/data/disk-files/pdf-doc-txt/easychair.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    # .zip
    ("/data/no-extension/file_zip", "application/zip"),
    ("/data/disk-files/archives/zip-with-docx-and-doc.zip", "application/zip"),
    ("/data/disk-files/archives/zip-with-some-smashed-bytes.zip", "application/zip"),
    ("/data/disk-files/archives/tim-and-merry/archives.zip", "application/zip"),
    ("/data/disk-files/archives/zip-with-password.zip", "application/zip"),
    ("/data/disk-files/archives/box.zip", "application/zip"),
    ("/data/disk-files/archives/tom/jail/jerry.zip", "application/zip"),
    ("/data/msg-5-outlook/archive-with-msg.zip", "application/zip"),
    # .7z
    ("/data/no-extension/file_7z", "application/x-7z-compressed"),
    ("/data/disk-files/archives/jerry/etc/jerry.7z", "application/x-7z-compressed"),
    ("/data/eml-7-recursive/d.7z", "application/x-7z-compressed"),
    # .rar
    ("/data/disk-files/archives/rar-with-pdf-doc-docx.rar", "application/x-rar"),
    # .tar.gz
    ("/data/no-extension/file_tar_gz", "application/gzip"),
    ("/data/disk-files/archives/targz-with-pdf-doc-docx.tar.gz", "application/gzip"),
    # .msg
    ("/data/no-extension/file_msg", "application/vnd.ms-outlook"),
    ("/data/msg-5-outlook/DISEARĂ-Te-așteptăm-la-discuția-despre-finanțarea-culturii.msg", "application/vnd.ms-outlook"),
    # .pst
    ("/data/no-extension/file_pst", "application/x-hoover-pst"),
    ("/data/pst/flags_jane_doe.pst", "application/x-hoover-pst"),
])
def test_blob_mime_types(testdata_relative_path, expected_mime_type):
    file_path = settings.SNOOP_TESTDATA + testdata_relative_path
    blob = models.Blob.create_from_file(file_path).mime_type
    assert blob == expected_mime_type
