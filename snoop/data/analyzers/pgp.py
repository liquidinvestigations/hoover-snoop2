from pathlib import Path
import subprocess
from django.conf import settings
from ..tasks import ShaormaBroken

gpghome = Path(settings.SNOOP_COLLECTION_ROOT).parent / 'gpghome'


def is_encrypted(data):
    return b'-----BEGIN PGP MESSAGE-----' in data


def decrypt(data):
    if not gpghome.exists():
        raise ShaormaBroken("No gpghome folder", 'gpg_not_configured')

    result = subprocess.run(
        ['gpg', '--home', gpghome, '--decrypt'],
        input=data,
        check=True,
        stdout=subprocess.PIPE,
    )
    return result.stdout


def import_keys(keydata):
    subprocess.run(
        ['gpg', '--home', gpghome, '--import'],
        input=keydata,
        check=True,
    )
