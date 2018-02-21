import subprocess
from django.conf import settings
from ..tasks import ShaormaBroken


def is_encrypted(data):
    return b'-----BEGIN PGP MESSAGE-----' in data


def decrypt(data):
    if not settings.SNOOP_GNUPG_HOME:
        raise ShaormaBroken("No SNOOP_GNUPG_HOME set", 'gpg_not_configured')

    result = subprocess.run(
        ['gpg', '--home', settings.SNOOP_GNUPG_HOME, '--decrypt'],
        input=data,
        check=True,
        stdout=subprocess.PIPE,
    )
    return result.stdout


def import_keys(keydata):
    subprocess.run(
        ['gpg', '--home', settings.SNOOP_GNUPG_HOME, '--import'],
        input=keydata,
        check=True,
    )
