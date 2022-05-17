"""Tasks that extract GPS location and other metadata from images.
"""

from datetime import datetime
from django.utils.timezone import utc
import exifread
from ..tasks import snoop_task, returns_json_blob, SnoopTaskBroken
from ..utils import zulu

EXIFREAD_MIME_TYPES = {'image/tiff', 'image/jpeg', 'image/webp', 'image/heic'}
"""Mime types supported for EXIF geographical data extraction.

Extracting exif data is done using [ExifRead](https://pypi.org/project/ExifRead/).
The supported filetypes can be found in the Project description.
"""


def can_extract(blob):
    """Checks if we can extract EXIF data from blob."""
    return blob.mime_type in EXIFREAD_MIME_TYPES


def extract_gps_location(tags):
    """Returns GPS "lat lon" string from dict with tags."""
    def ratio_to_float(ratio):
        return float(ratio.num) / ratio.den

    def convert(value):
        d = ratio_to_float(value.values[0])
        m = ratio_to_float(value.values[1])
        s = ratio_to_float(value.values[2])
        return d + (m / 60.0) + (s / 3600.0)

    tags = {key: tags[key] for key in tags.keys() if key.startswith('GPS')}

    lat = tags.get('GPS GPSLatitude')
    lat_ref = tags.get('GPS GPSLatitudeRef')
    lng = tags.get('GPS GPSLongitude')
    lng_ref = tags.get('GPS GPSLongitudeRef')

    if any(v is None for v in [lat, lat_ref, lng, lng_ref]):
        return None

    lat = convert(lat)
    if lat_ref.values and (lat_ref.values[0] != 'N'):
        lat = -lat
    lng = convert(lng)
    if lng_ref.values and (lng_ref.values[0] != 'E'):
        lng = -lng
    return "{}, {}".format(lat, lng)


def convert_exif_date(str):
    try:
        date = datetime.strptime(str, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None
    return zulu(utc.fromutc(date))


@snoop_task('exif.extract')
@returns_json_blob
def extract(blob):
    """Task to extract EXIF GPS tags from Blob with image."""

    # details=False removes thumbnails and MakerNote (manufacturer specific
    # information). See https://pypi.python.org/pypi/ExifRead#tag-descriptions

    with blob.open(need_seek=True) as f:
        try:
            tags = exifread.process_file(f, details=False)
        except (AttributeError, IndexError) as e:
            raise SnoopTaskBroken("ExifRead failed: " + str(e),
                                  "exifread_failed_attribute_index_error")

    if not tags:
        return {}

    data = {}
    try:
        gps = extract_gps_location(tags)
    except ZeroDivisionError as e:
        raise SnoopTaskBroken("zero division error when computing GPS: " + str(e),
                              "exifread_gps_zero_division_error")

    if gps:
        data['location'] = gps

    for key in ['EXIF DateTimeOriginal', 'Image DateTime']:
        if key in tags:
            date = convert_exif_date(str(tags[key]))
            if date:
                data['date-created'] = date
                break

    return data
