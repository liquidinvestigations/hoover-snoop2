"""Tasks to decrypt gpg email and import keys.

Requires the passphrase be removed from the key and imported into the "gpghome" directory under the
collection dataset root.
"""
import pathlib
import subprocess

from .. import collections
from ..tasks import SnoopTaskBroken


def is_encrypted(data):
    """Checks if string data encodes PGP encrypted message.

    Only works in the text representation (that begins with `-----BEGIN PGP MESSAGE-----`.
    any binary encodings will not work.
    """

    return b'-----BEGIN PGP MESSAGE-----' in data


def decrypt(data):
    """Runs `gpg --decrypt` on the given data with the given collection `gpghome` dir."""

    with collections.current().mount_gpghome() as gpghome:
        gpghome = pathlib.Path(gpghome)
        if not gpghome.exists():
            raise SnoopTaskBroken("No gpghome folder", 'gpg_not_configured')

        try:
            result = subprocess.run(
                ['gpg', '--home', gpghome, '--decrypt'],
                input=data,
                check=True,
                stdout=subprocess.PIPE,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            # This may as well be a non-permanent error, but we have no way to tell
            if e.output:
                output = e.output.decode('latin-1')
            else:
                output = "(no output)"
            raise SnoopTaskBroken('running gpg --decrypt failed: ' + output,
                                  'gpg_decrypt_failed')


def import_keys(keydata):
    """Runs `gpg --import` on the given key data, to be saved in the collection `gpghome`.

    This requires that the keydata be with passphrase removed.

    Arguments:
        keydata: data supplied to `gpg` process stdin
    """
    with collections.current().mount_gpghome() as gpghome:
        subprocess.run(
            ['gpg', '--home', gpghome, '--import'],
            input=keydata,
            check=True,
        )
