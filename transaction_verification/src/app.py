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
    def InitOrder(self, request, context):
        order_id = request.order_id
        incoming_vc = dict(
            request.vector_clock) if request.vector_clock else None

        order_data = {
            "card_number": request.card_number,
            "order_amount": request.order_amount,
            "items": list(request.items),
            "user_name": request.user_name
        }

        logger.info(f"InitOrder received for order_id={order_id}")
        order = state_manager.process_event(
            order_id, incoming_vc, **order_data)
        logger.info(
            f"VC after InitOrder for {order_id}: {order[VECTOR_CLOCK]}")

        broadcast(is_valid=True, vector_clock=order[VECTOR_CLOCK])

    # Event (a):
    def VerifyItems(self, request, context):
        order_id = request.order_id
        incoming_vc = dict(
            request.vector_clock) if request.vector_clock else None

        order = state_manager.process_event(order_id, incoming_vc)
        items = order.get("items", [])

        is_valid = len(items) > 0
        logger.info(
            f"VC after VerifyItems for {order_id}: {order[VECTOR_CLOCK]}")

        broadcast(is_valid=is_valid, vector_clock=order[VECTOR_CLOCK])

    # Event (b):
    def VerifyUserData(self, request, context):
        order_id = request.order_id
        incoming_vc = dict(
            request.vector_clock) if request.vector_clock else None

        order = state_manager.process_event(order_id, incoming_vc)
        user_name = order.get("user_name", "")

        is_valid = len(user_name) > 0
        logger.info(
            f"VC after VerifyUserData for {order_id}: {order[VECTOR_CLOCK]}")
        broadcast(is_valid=is_valid, vector_clock=order[VECTOR_CLOCK])

    # Event (c):
    def VerifyCreditCard(self, request, context):
        order_id = request.order_id
        incoming_vc = dict(
            request.vector_clock) if request.vector_clock else None

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
        
        broadcast(is_valid=is_valid, vector_clock=order[VECTOR_CLOCK])





    # TODO double check
    def ClearOrderData(self, request, context):
        order_id = request.order_id
        final_vc = dict(request.vector_clock)

        try:
            local_vc = state_manager.get_vector_clock(order_id)
        except KeyError:
            return transaction_verification.ClearResponse(is_cleared=True)

        is_safe_to_clear = all(local_vc.get(svc, 0) <= final_vc.get(
            svc, 0) for svc in state_manager.services)

        if is_safe_to_clear:
            state_manager.clear_order(order_id)
            logger.info(f"Data safely cleared for order {order_id}")
            return transaction_verification.ClearResponse(is_cleared=True)
        else:
            logger.error(
                f"Cannot clear data for {order_id}. Causality violation. Local: {local_vc}, Final: {final_vc}")
            return transaction_verification.ClearResponse(is_cleared=False)


class BroadcastService(broadcast_grpc.BroadcastService):
    def Broadcast(self, request, context):
        #call function 
        return 

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server)
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # 2 endpoints?
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastService(), server)
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
