from functools import wraps
import logging
import time

from django.conf import settings
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats import view as view_module
from opencensus.ext.prometheus import stats_exporter as prometheus
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module
from opencensus.ext.zipkin import trace_exporter as zipkin
from opencensus.trace.samplers import always_on, always_off
from opencensus.trace.tracer import Tracer


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_enabled():
    return getattr(settings, 'TRACING_ENABLED', False)


# Setup the exporter
ze = zipkin.ZipkinExporter(service_name = getattr(settings, 'TRACING_SERVICE', 'snoop'),
                    host_name = getattr(settings, 'TRACING_HOST', 'zipkin'),
                    port = getattr(settings, 'TRACING_PORT', 9411),
                    endpoint = getattr(settings, 'TRACING_API', '/api/v2/spans'))

# If enabled configure 100% sample rate, otherwise, 0% sample rate
if is_enabled():
    sampler = always_on.AlwaysOnSampler()
else:
    sampler = always_off.AlwaysOffSampler()

# Set the tracer to use the exporter
# Get the global singleton Tracer object
tracer = Tracer(exporter=ze, sampler=sampler)
tracer.span('root')
