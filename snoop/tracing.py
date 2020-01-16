import logging
from contextlib import contextmanager

from django.conf import settings
from opencensus.ext.zipkin import trace_exporter as zipkin
from opencensus.trace.samplers import always_on, always_off
from opencensus.trace.tracer import Tracer
from opencensus.trace import execution_context


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_enabled():
    return getattr(settings, 'TRACING_ENABLED', False)


# Setup the exporter
ze = zipkin.ZipkinExporter(service_name=getattr(settings, 'TRACING_SERVICE', 'snoop'),
                           host_name=getattr(settings, 'TRACING_HOST', 'zipkin'),
                           port=getattr(settings, 'TRACING_PORT', 9411),
                           endpoint=getattr(settings, 'TRACING_API', '/api/v2/spans'))

# If enabled configure 100% sample rate, otherwise, 0% sample rate
if is_enabled():
    sampler = always_on.AlwaysOnSampler()
else:
    sampler = always_off.AlwaysOffSampler()


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
def trace(name):
    tracer = Tracer(exporter=ze, sampler=sampler)
    try:
        with set_parent(tracer):
            with span(name):
                yield
    finally:
        tracer.finish()
        execution_context.clean()


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
