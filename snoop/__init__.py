import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
localsettings_path = ROOT / 'snoop' / 'localsettings.py'


def set_django_settings():
    if localsettings_path.exists():
        settings_module = "snoop.localsettings"

    else:
        settings_module = "snoop.defaultsettings"

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)
