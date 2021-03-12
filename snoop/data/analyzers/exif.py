"""Tasks that extract GPS location and other metadata from images.
"""

from datetime import datetime
from django.utils.timezone import utc
import exifread
from ..tasks import snoop_task, returns_json_blob
from ..utils import zulu


def can_extract(blob):
    return blob.mime_type.startswith('image/')


def extract_gps_location(tags):
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
    if lat_ref.values[0] != 'N':
        lat = -lat
    lng = convert(lng)
    if lng_ref.values[0] != 'E':
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
    # details=False removes thumbnails and MakerNote (manufacturer specific
    # information). See https://pypi.python.org/pypi/ExifRead#tag-descriptions

    with blob.open() as f:
        tags = exifread.process_file(f, details=False)

    if not tags:
        return {}

    data = {}
    gps = extract_gps_location(tags)
    if gps:
        data['location'] = gps

    for key in ['EXIF DateTimeOriginal', 'Image DateTime']:
        if key in tags:
            date = convert_exif_date(str(tags[key]))
            if date:
                data['date-created'] = date
                break

    return data
