import logging

from django.conf import settings
from opencensus.ext.zipkin import trace_exporter as zipkin
from opencensus.trace.samplers import always_on, always_off
from opencensus.trace.tracer import Tracer


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

# Set the tracer to use the exporter
# Get the global singleton Tracer object
def create_tracer():
    return Tracer(exporter=ze, sampler=sampler)
