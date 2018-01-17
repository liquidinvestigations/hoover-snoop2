#!/bin/bash -ex

sudo -u ubuntu PYTHONDONTWRITEBYTECODE=yesgoddamnit /opt/snoop2-venv/bin/py.test /opt/snoop2 "$@"
