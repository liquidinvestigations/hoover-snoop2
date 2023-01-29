"""Tracing integration library.

Provides init functions for hooking into different entry points, as well the ability to
wrap functions and create custom spans.
"""
import functools
import threading
import os
import subprocess
import logging
from contextlib import contextmanager

import uptrace
from opentelemetry import trace
from opentelemetry import metrics
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
# commented out because of error "TypeError: 'HttpHeaders' object does not support item assignment"
# from opentelemetry.instrumentation.requests import RequestsInstrumentor
# commented out because of leaked socket warning, but it seems to be harmless as it uses same fd
# from opentelemetry.instrumentation.celery import CeleryInstrumentor

SERVICE_NAME = "hoover-snoop"
SERVICE_VERSION = subprocess.check_output("git describe --tags --always", shell=True).decode().strip()

log = logging.getLogger(__name__)
threadlocal = threading.local()


def init_tracing(_from):
    """Initialize tracing at the beginning of an entry point, like manage.py, celery or gunicorn.

    The _from argument is logged at the command line.
    """
    log.info('FROM %s: initializing trace engine for %s %s...', _from, SERVICE_NAME, SERVICE_VERSION)
    if os.getenv('UPTRACE_DSN'):
        uptrace.configure_opentelemetry(
            service_name=SERVICE_NAME,
            service_version=SERVICE_VERSION,
        )
        LoggingInstrumentor().instrument(set_logging_format=True)
        Psycopg2Instrumentor().instrument(skip_dep_check=True)
        DjangoInstrumentor().instrument()
        # RequestsInstrumentor().instrument()
        # CeleryInstrumentor().instrument()


class Tracer:
    """Tracing handler with simplified interface.

    Manages flush of opentelemetry tracing objects after use.
    """
    def __init__(self, name, version=None):
        """Construct tracer with name and version.
        """
        self.name = name
        self.version = version or SERVICE_VERSION
        self.tracer = trace.get_tracer(self.name, self.version)
        self.meter = metrics.get_meter(self.name)
        self.counters = {}
        self.counter_attributes = {}

    @contextmanager
    def span(self, *args, **kwds):
        """Call the opentelemetry start_as_current_span() context manager and manage shutdowns.
        """
        log.debug('creating tracer for module %s...', self.name)
        try:
            with self.tracer.start_as_current_span(*args, **kwds) as span:
                yield span
        finally:
            log.debug('destroying tracer for module %s...', self.name)
            try:
                # flush data with timeout of 30s
                trace.get_tracer_provider().force_flush(30000)
            # the ProxyTracerProvider we get when no tracing is configured
            # doesn't have these methods.
            except AttributeError:
                pass
            except Exception as e:
                log.warning('tracer flush exception: ' + str(e))

    def wrap_function(self):
        """Returns a function wrapper that has a telemetry span around the function.
        """
        def decorator(function):
            fname = self.name + '.' + function.__qualname__
            log.debug('initializing trace for function %s...', fname)

            @functools.wraps(function)
            def wrapper(*k, **v):
                with self.span(fname) as _:
                    log.debug('executing traced function %s...', fname)
                    return function(*k, **v)
            return wrapper
        return decorator

    def count(self, key, value=1, attributes={}, description='', unit="1"):
        """Helper for the opentelemetry "_metrics" counter.
        """
        key = key.replace(' ', '_')
        if key not in self.counters:
            self.counters[key] = self.meter.create_counter(name=key, description=description, unit=unit)
        attributes = dict(attributes)
        attributes.update(self.counter_attributes)
        collection = getattr(threadlocal, 'collection', None)
        if collection:
            attributes['collection'] = collection.name
        self.counters[key].add(value, attributes=attributes)
