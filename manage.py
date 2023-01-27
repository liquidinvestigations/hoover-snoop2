#!/usr/bin/env python3
import os
import sys

import uptrace
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
# from opentelemetry.instrumentation.logging import LoggingInstrumentor

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")

    if os.getenv('UPTRACE_DSN'):
        uptrace.configure_opentelemetry(
            service_name="hoover-search",
            service_version="0.0.0",
        )
        # LoggingInstrumentor().instrument(set_logging_format=True)
        Psycopg2Instrumentor().instrument()
        DjangoInstrumentor().instrument()

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)
