import sys
import os
import logging

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures

logger = logging.getLogger(__name__)

# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class FraudDetectionService(fraud_detection_grpc.FraudDetectionService):
    # Create an RPC function to say hello
    def CheckFraud(self, request, context):
        # Create a HelloResponse object
        response = fraud_detection.FraudResponse()
        card_number: str = request.card_number
        order_amount: int = int(request.order_amount)
        logger.info(f"Fraud check request arrived with: card number: {card_number} and order amount: {order_amount}")
        is_fraud = False
        response.is_fraud = is_fraud 
        logger.info(f"Is the transaction a fraud?: {is_fraud}") 
        # Set the greeting field of the response object
        # Print the greeting message
        # Return the response object
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50051.")
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
