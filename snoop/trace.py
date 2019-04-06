from functools import wraps
import logging
import time

from django.conf import settings
from opencensus.stats import aggregation as aggregation_module
from opencensus.stats import measure as measure_module
from opencensus.stats import stats as stats_module
from opencensus.stats import view as view_module
from opencensus.stats.exporters import prometheus_exporter as prometheus
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module
from opencensus.trace.exporters.zipkin_exporter import ZipkinExporter
from opencensus.trace.samplers import always_on, always_off
from opencensus.trace.tracer import Tracer


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_enabled():
    return getattr(settings, 'TRACING_ENABLED', False)


# Setup the exporter
ze = ZipkinExporter(service_name = getattr(settings, 'TRACING_SERVICE', 'snoop'),
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

# Create the measures
# The latency in milliseconds
m_latency_ms = measure_module.MeasureFloat("latency", "The latency in milliseconds", "ms")

stats = stats_module.Stats()

# Create the method key
key_method = tag_key_module.TagKey("method")
# Create the status key
key_status = tag_key_module.TagKey("status")
# Create the error key
key_error = tag_key_module.TagKey("error")

if is_enabled():
    latency_view = view_module.View("latency", "The distribution of the latencies",
        [key_method, key_status, key_error],
        m_latency_ms,
        aggregation_module.DistributionAggregation())

    call_count_view = view_module.View("call_count", "The number of calls per function",
        [key_method, key_status, key_error],
        m_latency_ms,
        aggregation_module.CountAggregation())

    exporter = prometheus.new_stats_exporter(prometheus.Options(namespace="snoop", port=8000))
    stats.view_manager.register_exporter(exporter)
    stats.view_manager.register_view(latency_view)
    stats.view_manager.register_view(call_count_view)


class TracerStats():
    start = None
    end = None
    interval = None
    annotation = None

    def __init__(self, annotation=None):
        self.annotation = annotation or 'default'

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, traceback):
        self.end = time.time()
        self.interval = self.end - self.start
        if not is_enabled():
            return

        mmap = stats.stats_recorder.new_measurement_map()
        mmap.measure_float_put(m_latency_ms, self.interval)

        tmap = tag_map_module.TagMap()
        if exc_type:
            tmap.insert(key_error, tag_value_module.TagValue('error'))
            tmap.insert(key_status, tag_value_module.TagValue('error'))
        else:
            tmap.insert(key_method, tag_value_module.TagValue(self.annotation))
            tmap.insert(key_status, tag_value_module.TagValue('success'))

        tmap.insert(key_method, tag_value_module.TagValue(self.annotation))
        mmap.record(tmap)
        logger.info('record trace stats %s' % tmap)


def traced(annotation=None):

    def decorator(func):

        @wraps(func)
        def profile_function(*args, **kwargs):
            with TracerStats() as tracer_stats:
                tracer_stats.annotation = annotation
                return func(*args, **kwargs)

        return profile_function

    return decorator
