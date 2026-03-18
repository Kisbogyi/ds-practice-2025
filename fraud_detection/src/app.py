import sys
import logging
import time

import broadcast.broadcast_pb2 as broadcast
import broadcast.broadcast_pb2_grpc as broadcast_grpc

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
import fraud_detection.fraud_detection_pb2 as fraud_detection
import fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures

logger = logging.getLogger(__name__)

# username - unix timestamp
transaction_log: dict[str, list[float]] = {}

def add_to_transaction_log(username: str):
    if username in transaction_log.keys():
        transaction_log[username].append(time.time())
    else:
        transaction_log[username] = [time.time()]

def get_transaction_history(username: str) -> list[float]:
    if username in transaction_log.keys():
        return transaction_log[username]
    else:
        return []

def calculate_risk(username: str, transaction_amount: int, billing_address: str, transaction_history: list[float]) -> float:
    risk:float = .0
    risk += transaction_amount * 0.5
    
    now = time.time()
    last_week = now - (7 * 24 * 60 * 60)
    transactions_last_week = len(list(filter(lambda transaction_timestamp: transaction_timestamp > last_week, transaction_history)))
    risk += transactions_last_week * 5

    if len(transaction_history) == 0:
        risk += 25

    # Dummy value should need all European union country names
    if "USA" not in billing_address or "EU" not in billing_address:
        risk += 25
    return risk


# Create a class to define the server functions, derived from
# fraud_detection_pb2_grpc.HelloServiceServicer
class FraudDetectionService(fraud_detection_grpc.FraudDetectionService):
    # Create an RPC function to say hello
    def CheckFraud(self, request, context):
        # Create a HelloResponse object
        response = fraud_detection.FraudResponse()
        order_amount: int = request.order_amount
        username: str = request.username
        billing_address: str = request.billing_address
        logger.info(f"Fraud check request arrived with:  username: {username}, order amount: {order_amount}, billing_address: {billing_address}")
        transaction_history = get_transaction_history(username)
        risk = calculate_risk(username, order_amount, billing_address, transaction_history)
        logger.info(f"Risk is: {risk}")
        is_fraud = risk >= 55
        response.is_fraud = is_fraud 
        add_to_transaction_log(username) 
        logger.info(f"Is the transaction a fraud?: {is_fraud}") 

        # Set the greeting field of the response object
        # Print the greeting message
        # Return the response object
        return response


class BroadcastService(broadcast_grpc.BroadcastService):
    def Broadcast(self, request, context):
        #call function 
        return 

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastService(), server)
    port = "50054"
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
