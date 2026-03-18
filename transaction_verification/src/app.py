import sys
import logging
import grpc
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

import utils.pb.broadcast.broadcast_pb2 as broadcast
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc


logger = logging.getLogger(__name__)

def luhn_verifier(card_number: str) -> bool:
    sum = 0
    for i, digit in enumerate(card_number[:-1]):
        if i % 2 == 0:
            doubled = int(digit) * 2
            if doubled < 10:
                sum += doubled
            else:
                sum += (doubled % 10) + (doubled // 10)
        else:
            sum += int(digit)
    sum += int(card_number[-1])
    logger.info(f"Luhn checksum is: {sum}")
    return (sum % 10) == 0
    
def length_check(card_number: str) -> bool:
    has_good_length =  len(card_number) == 16
    logger.info(f"Length scheck status: {has_good_length}")
    return has_good_length


# Create a class to define the server functions, derived from
# transaction_verification_pb2_grpc.HelloServiceServicer
class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationService):
    # Create an RPC function to say hello
    def VerifyTransaction(self, request, context):
        # Create a HelloResponse object
        card_number: str = request.card_number
        order_amount: int = request.order_amount
        logger.info(f"Transaction verification request arrived with: card number: {card_number} and order amount: {order_amount}")
        response = transaction_verification.VerificationResponse()
        is_valid = order_amount > 0 and order_amount < 100 and length_check(card_number) and luhn_verifier(card_number)
        response.is_valid = is_valid 
        if order_amount <= 0:
            logger.info(f"The order amount: {order_amount} is to small")
        if order_amount >= 100:
            logger.info(f"The order amount: {order_amount} is to big")

        # Set the greeting field of the response object
        logger.info(f"Is the ransaction valid?: {is_valid}") 
        return response

class BroadcastService(broadcast_grpc.BroadcastService):
    def Broadcast(self, request, context):
        #call function 
        return 

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(TransactionVerificationService(), server)
    # Listen on port 50052
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # 2 endpoints?
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastService(), server)
    port = "50054"
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
