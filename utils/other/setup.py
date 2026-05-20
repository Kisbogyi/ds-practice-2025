import logging
import os
import sys

from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader


# Service name is required for most backends
resource = Resource.create(attributes={
    SERVICE_NAME: "bookshop"
})

tracerProvider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="observability/v1/traces"))
tracerProvider.add_span_processor(processor)
trace.set_tracer_provider(tracerProvider)

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="observability/v1/metrics")
)
meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meterProvider)


def initialize_pb_paths():
    pb_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '../../utils/pb'))
    for root, dirs, files in os.walk(pb_path):
        sys.path.append(root)


def getLogger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def get_debug_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)


    # if logger.hasHandlers():
    #     logger.handlers.clear()

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # logger.propagate = False
    return logger
