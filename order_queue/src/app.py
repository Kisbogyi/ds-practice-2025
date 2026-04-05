import order_queue_pb2
import order_queue_pb2_grpc
import queue

class OrderQueueService:
    def __init__:
    self._queue = queue.PriorityQueue() # magic python queue
    
    def Enqueue(self, queue_item):
        _queue.put(queue_item)

    def Dequeue(self):
        queue_item = _queue.get()
        # rpc give over item
        ...
        # wait until task done, possibly through broadcast (?)
        _queue.task_done()

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    order_queue_pb2_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueService(), server)
    # Listen on port 50061
    port = "50061"
    server.add_insecure_port("[::]:" + port)
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastService(), server)
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.debug("Server started. Listening on port 50061.")
    # Keep thread alive
    server.wait_for_termination()