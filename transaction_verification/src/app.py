import sys
import os
import logging

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

import grpc
from concurrent import futures

logger = logging.getLogger(__name__)

# Create a class to define the server functions, derived from
# transaction_verification_pb2_grpc.HelloServiceServicer
class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationService):
    # Create an RPC function to say hello
    def VerifyTransaction(self, request, context):
        # Create a HelloResponse object
        card_number: str = request.card_number
        order_amount: int = int(request.order_amount)
        logger.info(f"Transaction verification request arrived with: card number: {card_number} and order amount: {order_amount}")
        response = transaction_verification.VerificationResponse()
        is_valid = False
        response.is_valid = is_valid 
        # Set the greeting field of the response object
        logger.info(f"Is the ransaction valid?: {is_valid}") 
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(TransactionVerificationService(), server)
    # Listen on port 50052
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.debug("Server started. Listening on port 50052.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    serve()
