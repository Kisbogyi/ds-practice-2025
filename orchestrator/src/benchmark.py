import time
from opentelemetry import metrics
import functools

meter = metrics.get_meter("bookshop")
startup_counter = meter.create_histogram(
    "bookshop.duration",
    description="measures the duration of incoming HTTP requests",
    unit="ns",
)


def timeit(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.monotonic_ns()
        result = await func(*args, **kwargs)
        end = time.monotonic_ns()
        startup_counter.record(end - start)
        return result
    return wrapper
