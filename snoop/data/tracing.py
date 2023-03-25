"""Tracing integration library.

Provides init functions for hooking into different entry points, as well the ability to
wrap functions and create custom spans.
"""
import functools
import os
import subprocess
import logging
from contextlib import contextmanager
from time import time


log = logging.getLogger(__name__)

try:
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

except Exception as e:
    log.exception(e)


SERVICE_NAME = "hoover-snoop"
try:
    SERVICE_VERSION = subprocess.check_output("git describe --tags --always", shell=True).decode().strip()
except Exception as e:
    log.exception(e)
    SERVICE_VERSION = 'unknown'

MAX_KEY_LEN = 63
"""Max key length for open telemetry counters, span names and other identifiers."""

MAX_COUNTER_KEY_LEN = 11
"""Max key reserved for counter suffixes."""


def init_tracing(_from):
    """Initialize tracing at the beginning of an entry point, like manage.py, celery or gunicorn.

    The _from argument is logged at the command line.
    """
    log.info('FROM %s: initializing trace engine for %s %s...', _from, SERVICE_NAME, SERVICE_VERSION)
    if os.getenv('UPTRACE_DSN'):
        try:
            uptrace.configure_opentelemetry(
                service_name=SERVICE_NAME,
                service_version=SERVICE_VERSION,
            )
            LoggingInstrumentor().instrument(set_logging_format=True)
            Psycopg2Instrumentor().instrument(skip_dep_check=True)
            DjangoInstrumentor().instrument()
            # RequestsInstrumentor().instrument()
            # CeleryInstrumentor().instrument()
        except Exception as e:
            log.exception(e)


def shorten_name(string, length):
    """Shortens a string to fit under some length.

    This is needed because opentelemetry key length limit are 64,
    and will fail in various ways if they're not.
    """
    if len(string) <= length:
        return string
    half_len = int((length - 5) / 2)
    string = string[:half_len] + '...' + string[-half_len + 1:]
    assert len(string) <= length
    return string


class Tracer:
    """Tracing handler with simplified interface.

    Manages flush of opentelemetry tracing objects after use.
    """
    def __init__(self, name, version=None):
        """Construct tracer with name and version.
        """
        name = name.replace(' ', '_')
        self.name = name
        self.version = version or SERVICE_VERSION
        try:
            self.tracer = trace.get_tracer(self.name, self.version)
            self.meter = metrics.get_meter(self.name)
        except Exception as e:
            log.exception(e)
            self.tracer = None
            self.meter = None
        self.counters = {}

    @contextmanager
    def span(self, name, *args, attributes={}, extra_counters={}, **kwds):
        """Call the opentelemetry start_as_current_span() context manager and manage shutdowns.
        """
        name = name.replace(' ', '_')
        if not name.startswith(self.name):
            name = self.name + '.' + name
        name = shorten_name(name, MAX_KEY_LEN - MAX_COUNTER_KEY_LEN - 3)  # -2 for the __
        log.debug('creating tracer for module=%s with name=%s...', self.name, name)

        attributes = self._populate_attributes(attributes)
        self.count(name + '__hits', attributes=attributes)
        for key, value in extra_counters.items():
            assert len(key) <= MAX_COUNTER_KEY_LEN, 'counter key too long!'
            self.count(name + '__' + key, value=value['value'], attributes=attributes, unit=value['unit'])
        t0 = time()
        try:
            with self.tracer.start_as_current_span(name, *args, **kwds) as span:
                yield span
        finally:
            self.count(name + '__duration', value=time() - t0, attributes=attributes, unit='s')
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

    def count(self, key, value=1, attributes={}, description='', unit="1", **kwds):
        """Helper for the opentelemetry "_metrics" counter.
        """
        key = key.replace(' ', '_')
        assert len(key) <= MAX_KEY_LEN, 'counter name too long!'

        try:
            if key not in self.counters:
                self.counters[key] = self.meter.create_counter(
                    name=key, description=description, unit=unit)
            attributes = self._populate_attributes(attributes)
            self.counters[key].add(value, attributes=attributes)
        except Exception as e:
            log.error('failed to increment count for counter %s: %s', key, str(e))

    def _populate_attributes(self, attributes):
        from snoop.threadlocal import threadlocal
        attributes = dict(attributes)
        collection = getattr(threadlocal, 'collection', None)
        if collection:
            attributes['collection'] = collection.name
        return attributes
