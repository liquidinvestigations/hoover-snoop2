import subprocess

from .. import collections
from ..tasks import ShaormaBroken


def is_encrypted(data):
    return b'-----BEGIN PGP MESSAGE-----' in data


def decrypt(data):
    gpghome = collections.current().gpghome_path
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
    gpghome = collections.current().gpghome_path
    subprocess.run(
        ['gpg', '--home', gpghome, '--import'],
        input=keydata,
        check=True,
    )
