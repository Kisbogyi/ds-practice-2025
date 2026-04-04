from concurrent import futures
import grpc
import sys
import logging
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

# import utils.pb.broadcast.broadcast_pb2 as broadcast
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc

from utils.other.orderStateManager import OrderStateManager, VECTOR_CLOCK
from utils.broadcast import broadcast

logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="verification_service")


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
    return (sum % 10) == 0


def length_check(card_number: str) -> bool:
    return len(card_number) == 16


class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationServiceServicer):

    def init_order(self, order_id: str, trigger_vc: dict, order_data: dict) -> dict:
        # ill update so trigger_vc is used when all works
        state_manager.get_or_create_order(order_id, None, order_data)
        return trigger_vc # FIXME placeholder needs to be increased on verification_service by 3

    def clear_order(self, order_id: str, incoming_vc: dict):
        state_manager.clear_order(order_id, incoming_vc)

    async def handle_broadcast(self, order_id: str, incoming_vc: dict):
        vc = state_manager.get_or_create_order(order_id, incoming_vc, None)[VECTOR_CLOCK]
        if vc['orchestrator'] == 1 and vc['verification_service'] == 0:  # entry point
            self.VerifyItems(order_id, vc)  # event A
            self.VerifyUserData(order_id, vc)  # event B
        elif vc['verification_service'] == 2:
            self.VerifyItems(order_id, vc)  # event C
        elif vc['verification_service'] == 3: # if all passed -> status is valid
            pass # TODO send success via orchestrator.set_transaction_status(order_id, True)
    
    # Event (a):
    def VerifyItems(self, order_id: str, incoming_vc: dict):
        order = state_manager.process_event(order_id, incoming_vc)
        items = order.get("items", [])

        is_valid = len(items) > 0
        logger.info(f"VC after VerifyItems for {order_id}: {order[VECTOR_CLOCK]}")

        if is_valid:
            broadcast(order_id, order[VECTOR_CLOCK])
        else: 
            pass # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")

    # Event (b):
    def VerifyUserData(self, order_id: str, incoming_vc: dict):
        order = state_manager.process_event(order_id, incoming_vc)
        user_name = order.get("user_name", "")

        is_valid = len(user_name) > 0
        logger.info(
            f"VC after VerifyUserData for {order_id}: {order[VECTOR_CLOCK]}")

        if is_valid:
            broadcast(order_id, order[VECTOR_CLOCK])
        else: 
            pass # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")


    # Event (c):
    def VerifyCreditCard(self, order_id: str, incoming_vc: dict):
        order = state_manager.process_event(order_id, incoming_vc)
        card_number = order.get("card_number", "")
        order_amount = order.get("order_amount", 0)

        checks = [
            (order_amount > 0, "Amount too small"),
            (order_amount < 100, "Amount too big"),
            (length_check(card_number), "Invalid length"),
            (luhn_verifier(card_number), "Luhn verification failed"),
        ]

        reason = next((msg for ok, msg in checks if not ok), None)
        is_valid = reason is None

        if not is_valid:
            logger.warning(
                f"Credit card verification failed for {order_id}: {reason}")

        logger.info(
            f"VC after VerifyCreditCard for {order_id}: {order[VECTOR_CLOCK]}")

        if is_valid:
            broadcast(order_id, order[VECTOR_CLOCK])
        else: 
            pass # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")


class BroadcastService(broadcast_grpc.BroadcastService):  # FIXME !!!!!!!
    def __init__(self, service):
        self.service = service

    def Broadcast(self, request, context):
        # TODO
        return


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    service = TransactionVerificationService()
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(
        service, server)
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # 2 endpoints?

    broadcast_grpc.add_BroadcastServiceServicer_to_server(
        BroadcastService(service), server)
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.debug(f"Server started. Listening on port {port}.")
    server.wait_for_termination()


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    serve()
