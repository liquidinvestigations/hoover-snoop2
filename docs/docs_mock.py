"""Mocks out modules when building documentation.
"""

import sys
from unittest.mock import MagicMock as mock


def mock_all():
    with open("./docs/mock-modules.txt", 'r') as f:
        for x in f.readlines():
            x = x.strip()
            print(x, file=sys.stderr)
            sys.modules[x] = mock()


class EmptyClass:
    pass


if __name__ == '__main__':
    mock_all()
