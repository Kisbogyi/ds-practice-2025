import grpc
from concurrent import futures
import logging
import sys

import commitment.commitment_pb2_grpc as payment_pb2_grpc
import commitment.commitment_pb2 as payment_pb2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class PaymentService(payment_pb2_grpc.CommitmentSchemeServicer):
    def __init__(self):
        self.prepared = False

    def Prepare(self, request, context):
        logger.info("Prepared")
        self.prepared = True
        return payment_pb2.PrepareResponse(ready=True)
    
    def Commit(self, request, context):
        if self.prepared:
            logger.info(f"Payment commited for order {request.order_id}")
            self.prepared = False
            succeded = True
        else:
            logger.info("Not prepared but commit was called")
            succeded = False
        return payment_pb2.CommitResponse(success=succeded)

    def Abort(self, request, context):
        self.prepared = False
        logger.info(f"Payment aborted for order {request.order_id}")
        return payment_pb2.AbortResponse(aborted=True)

def start():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())

    service = PaymentService()
    payment_pb2_grpc.add_CommitmentSchemeServicer_to_server(
        service, server
    )

    port = "50073"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == "__main__":
    logger.info("service started")
    start()
