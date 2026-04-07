import threading
import time
import grpc
import logging
from concurrent import futures
import sys
from typing import Never

import order_executor.bullying_pb2_grpc as bullying_grpc
from heartbeat import HeartbeatService, healthcheck
from bullying import CoordinatorService, ElectionService, bully, get_container_ip

import order_queue.order_queue_pb2 as order_queue_pb2
import order_queue.order_queue_pb2_grpc as order_queue_pb2_grpc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class ExecutorService:
    leader_ip: str 

    def __init__(self, ques_stub) -> None:
        self.ques_stub = ques_stub
        # highest ip in current subnet
        self.leader_ip = "10.89.13.1"

    def start_leader_election(self):
        self.leader_election()

    def leader_election(self) -> Never:
        logger.info("started leader election")
        while True:
            self.que_operations()
            if not healthcheck(self.leader_ip):
                logger.info(f"leader: {self.leader_ip} failed healthcheck")
                bully(self)
            else:
                logger.info(f"leader: {self.leader_ip} was healthy")
            time.sleep(5)

    def que_operations(self):
        if self.leader_ip == get_container_ip():
            with grpc.insecure_channel('order_queue:50061') as channel:
                # Create a stub object.
                stub = order_queue_pb2_grpc.OrderQueueServiceStub(channel)
                # Call the service through the stub object.
                que_item = stub.Dequeue(order_queue_pb2.DequeueRequest())
            # _data = self.ques_stub.deque()
            logger.info(f"Order beeing executed: {que_item} ...")

    def start(self):
        # Create a gRPC server
        server = grpc.server(futures.ThreadPoolExecutor())
        # Add HelloService
        bullying_grpc.add_HeartbeatServiceServicer_to_server(
            HeartbeatService(), server
        )
        bullying_grpc.add_CoordinatorServicer_to_server(
            CoordinatorService(self), server
        )
        bullying_grpc.add_ElectionServicer_to_server(
            ElectionService(self), server
        )
        port = "50070"
        server.add_insecure_port("[::]:" + port)
        server.start()
        logger.info(f"Server started. Listening on port {port}.")
        # Keep thread alive
        server.wait_for_termination()


if __name__ == "__main__":
    logger.info("service started")
    svc = ExecutorService(ques_stub=None)
    t = threading.Thread(target=svc.start)
    t.start()
    svc.start_leader_election()


