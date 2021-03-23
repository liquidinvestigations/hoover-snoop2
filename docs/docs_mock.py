"""Mocks out modules when building documentation.
"""

import sys
from unittest.mock import MagicMock


class SuperMagicMock(MagicMock):
    """MagicMock, but with __version__ too.
    """

    __version__ = '3.5.4'
    """Mock a high value for this too.

    Django checks the version of the Postgres client python lib when it boots up. It wants to see something
    >= 2.5.4 last I checked.
    """


def mock_all():
    libs = set()
    with open("./docs/requirements-mkdocs.txt") as f:
        for line in f.readlines():
            if '==' in line:
                x = line.split('==')[0]
                libs.add(x)

    with open("./docs/mock-modules.txt", 'r') as f:
        for x in f.readlines():
            x = x.strip()

            if x.startswith('#'):
                continue

            if x.split('.')[0] in libs:
                print('skip lib=' + x, file=sys.stderr)
                continue

            if x.split('.')[-1][0].isupper():
                cla = x.split('.')[-1]
                mod = ".".join(x.split('.')[:-1])
                print('mod=' + mod, file=sys.stderr)
                print('cla=' + cla, file=sys.stderr)
                setattr(sys.modules[mod], cla, SuperMagicMock())
            print('mocking: ' + x, file=sys.stderr)
            sys.modules[x] = SuperMagicMock()()

    import pytkdocs.loader
    pytkdocs.loader.Loader.get_marshmallow_field_documentation = lambda *x, **y: SuperMagicMock()
    pytkdocs.loader.Loader.detect_field_model = lambda *x, **y: False


if __name__ == '__main__':
    mock_all()
