"""Settings and breakpoint for remote debugger.

TODO:
    This is not actively used in the system and should be removed.
"""
import logging
from contextlib import contextmanager

from django.conf import settings
from opencensus.ext.zipkin import trace_exporter as zipkin
from opencensus.trace.samplers import AlwaysOnSampler, AlwaysOffSampler
from opencensus.trace.tracer import Tracer
from opencensus.trace import execution_context


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_enabled():
    return getattr(settings, 'TRACING_ENABLED', False)


def create_exporter(service_name):
    return zipkin.ZipkinExporter(
        service_name=service_name,
        host_name=getattr(settings, 'TRACING_HOST', 'zipkin'),
        port=getattr(settings, 'TRACING_PORT', 9411),
        endpoint=getattr(settings, 'TRACING_API', '/api/v2/spans'),
    )


_exporters = {}


def get_exporter(service_name):
    if service_name not in _exporters:
        _exporters[service_name] = create_exporter(service_name)
    return _exporters[service_name]


# If enabled configure 100% sample rate, otherwise, 0% sample rate
if is_enabled():
    sampler = AlwaysOnSampler()
else:
    sampler = AlwaysOffSampler()


_threadlocal_parent = 'snoop2.parent'


def get_parent():
    return execution_context.get_opencensus_attr(_threadlocal_parent)


@contextmanager
def set_parent(value):
    old = get_parent()
    execution_context.set_opencensus_attr(_threadlocal_parent, value)
    try:
        yield
    finally:
        execution_context.set_opencensus_attr(_threadlocal_parent, old)


@contextmanager
def trace(name, service_name='snoop'):
    tracer = Tracer(exporter=get_exporter(service_name), sampler=sampler)
    try:
        # if there is another trace on the stack, mask it, so this trace's
        # spans don't interfere with it
        with set_parent(None):
            with set_parent(tracer):
                with span(name):
                    yield
    finally:
        tracer.finish()


@contextmanager
def span(name):
    parent = get_parent()
    if parent is None:
        yield

    else:
        span = parent.span(name)
        with set_parent(span):
            with span:
                yield


def add_annotation(text):
    parent = get_parent()
    if parent:
        parent.add_annotation(text)


def add_attribute(key, value):
    parent = get_parent()
    if parent:
        parent.add_attribute(key, value)
