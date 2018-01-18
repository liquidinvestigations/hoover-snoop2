#!/usr/bin/env python
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
localsettings = ROOT / 'snoop' / 'localsettings.py'

if __name__ == "__main__":
    if localsettings.exists():
        settings_module = "snoop.localsettings"
    else:
        settings_module = "snoop.defaultsettings"

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)
