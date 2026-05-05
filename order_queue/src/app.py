import os
import queue
import grpc
from concurrent import futures
import logging
import sys

import utils.other.setup as setup
setup.initialize_pb_paths() # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.order_que.order_queue_pb2 as order_queue_pb2
import utils.pb.order_que.order_queue_pb2_grpc as order_queue_pb2_grpc

# initialize logger
logger = setup.get_debug_logger(__name__)


class OrderQueueService(order_queue_pb2_grpc.OrderQueueServiceServicer):
    def __init__(self):
        # OrderQueueService
        # This que is thread safe, we can see it in the source code that
        # each operation start with locking the que
        self._queue = queue.PriorityQueue() # magic python queue
    
    def Enqueue(self, request, context):
        queue_item = request.order_id
        response = order_queue_pb2.EnqueueResponse()
        try:
            self._queue.put(queue_item)
            response.success = True
        except queue.Full:
            response.success = False
        return response 

    def Dequeue(self, request, context):
        # _ = request.order_id
        queue_item = self._queue.get()
        # we currently just log it if bigger tasks are there then this can be 
        #  implemented
        self._queue.task_done()
        response = order_queue_pb2.DequeueResponse() 
        response.order_id = queue_item
        return response


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    order_queue_pb2_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueService(), server)
    # Listen on port 50061
    port = "50061"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.debug("Server started. Listening on port 50061.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == "__main__":
    logger.info("service started")
    serve()
