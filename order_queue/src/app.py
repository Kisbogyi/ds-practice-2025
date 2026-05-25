import queue
import grpc
from concurrent import futures
import asyncio

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry import metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

import utils.other.setup as setup
setup.initialize_pb_paths()  # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.order_que.order_queue_pb2_grpc as order_queue_pb2_grpc
import utils.pb.order_que.order_queue_pb2 as order_queue_pb2
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2
from utils.other.orderStateManager import OrderStateManager

# initialize logger
logger = setup.get_debug_logger(__name__)
state_manager = OrderStateManager(service_name="order_queue_service")


meter = metrics.get_meter("bookshop")
queue_size_logger = meter.create_up_down_counter(
    "bookshop.order_queue_size",
    description="Number of items in the order queue"
)


class OrderQueueService(order_queue_pb2_grpc.OrderQueueServiceServicer):
    def __init__(self):
        # OrderQueueService
        # This que is thread safe, we can see it in the source code that
        # each operation start with locking the que
        self._queue = queue.PriorityQueue()  # magic python queue

    async def InitOrder(self, request, context):
        order_data = {
            "order_id": request.order_id,
            "user_name": request.user_name,
            "card_number": request.card_number,
            "billing_address": request.billing_address,
            "order": dict(request.order),
        }
        logger.info(f"Init order {request.order_id}: {order_data}")
        await state_manager.store_data(request.order_id, order_data, request.vc)
        completionVC = await state_manager.get_final_vc(request.order_id, ticks=1)
        return order_queue_pb2.completionVC(vc=completionVC)

    async def handle_broadcast(self, order_id: str, incoming_vc: list[int]):
        match await state_manager.get_triggered_clock(order_id, incoming_vc):
            case 0:
                logger.info(f"Order {order_id} {incoming_vc}: Triggering Order Queue")
                asyncio.create_task(self._enqueue(order_id))

    async def ClearOrder(self, request, context):
        logger.info(f"Clear order: {request.order_id}")
        success = await state_manager.clear_data(request.order_id, request.vc)
        return order_queue_pb2.clearStatus(success=success)

    def Enqueue(self, request, context):
        order_id = request.order_id
        response = order_queue_pb2.EnqueueResponse()
        try:
            self._enqueue(order_id)
            response.success = True
        except queue.Full:
            response.success = False
        return response

    async def _enqueue(self, order_id: str):
        logger.info(f"enqueue: {order_id}")
        order = await state_manager.get_data(order_id)
        if not order:
            logger.error(f"No data found for order_id={order_id}")
            return
        queue_item = {
            "order_id": order_id,
            "user_name": order.get("user_name", ""),
            "card_number": order.get("card_number", ""),
            "billing_address": order.get("billing_address", ""),
            "order": order.get("order", {}),
        }
        self._queue.put(queue_item)
        queue_size_logger.add(1)

    def Dequeue(self, request, context):
        try:
            queue_item = self._queue.get_nowait()
        except Exception:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Queue is empty")
            return order_queue_pb2.DequeueResponse()

        self._queue.task_done()
        queue_size_logger.add(-1)
        
        return order_queue_pb2.DequeueResponse(
            order_id=queue_item["order_id"],
            user_name=queue_item["user_name"],
            card_number=queue_item["card_number"],
            billing_address=queue_item["billing_address"],
            order=queue_item["order"],
        )
        
    
class BroadcastHandler(broadcast_grpc.BroadcastServiceServicer):
    def __init__(self, cls: OrderQueueService):
        self.cls = cls

    async def BroadcastVC(self, request, context):
        asyncio.create_task(self.cls.handle_broadcast(
            request.order_id, request.vector_clock))
        return broadcast_pb2.Empty()


async def serve():
    # Create a gRPC server
    # server = grpc.server(futures.ThreadPoolExecutor())
    server = grpc.aio.server()
    # Add HelloService
    service = OrderQueueService()
    order_queue_pb2_grpc.add_OrderQueueServiceServicer_to_server(
        service, server)
    # Listen on port 50061
    port = "50061"
    server.add_insecure_port("[::]:" + port)

    broadcast_grpc.add_BroadcastServiceServicer_to_server(
        BroadcastHandler(service), server
    )
    # broadcast_grpc.add_BroadcastClearServicer_to_server(
    #     BroadcastClearHandler(service), server
    # )
    port = "50054"
    server.add_insecure_port("[::]:" + port)

    # Start the server
    await server.start()
    logger.debug("Server started. Listening on port 50061.")
    # Keep thread alive
    await server.wait_for_termination()

if __name__ == "__main__":

    resource = Resource.create(attributes={
        SERVICE_NAME: "bookshop"
    })

    tracerProvider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://observability:4318/v1/traces"))
    tracerProvider.add_span_processor(processor)
    trace.set_tracer_provider(tracerProvider)

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint="http://observability:4318/v1/metrics")
    )
    meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meterProvider)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(serve())
