import asyncio
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

from utils.other.orderStateManager import OrderStateManager

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

    async def init_order(self, order_id: str, trigger_vc: list[int], order_data: dict) -> dict:
        await state_manager.store_data(order_id, order_data, trigger_vc)
        return state_manager.get_final_vc(order_id, 3)

    async def clear_order(self, order_id: str, incoming_vc: list[int]):
        return await state_manager.clear_data(order_id, incoming_vc)

    # TODO add async or threads
    async def handle_broadcast(self, order_id: str, incoming_vc: list[int]):
        if await state_manager.is_vc_triggered(order_id, incoming_vc, 0):
            self.VerifyItems(order_id, incoming_vc)  # event A
            self.VerifyUserData(order_id, incoming_vc)  # event B
        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 1):
            self.VerifyItems(order_id, incoming_vc)  # event C
        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 3):
            # all passed
            # TODO send success via orchestrator.set_transaction_status(order_id, True)
            pass

    # Event (a):
    async def VerifyItems(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        items = order.get("items", [])

        is_valid = len(items) > 0

        if not is_valid:
            # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")
            return

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(
            f"VC after VerifyItems for {order_id}: {await state_manager.get_vc(order_id)}")

    # Event (b):
    async def VerifyUserData(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        user_name = order.get("user_name", "")

        is_valid = len(user_name) > 0

        if not is_valid:
            # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")
            return

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(
            f"VC after VerifyUserData for {order_id}: {await state_manager.get_vc(order_id)}")

    # Event (c):
    async def VerifyCreditCard(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.process_event(order_id)
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
            # TODO send success via orchestrator.set_transaction_status(order_id, False, "Some reason")
            return

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(
            f"VC after VerifyCreditCard for {order_id}: {await state_manager.get_vc(order_id)}")


class BroadcastService(broadcast_grpc.BroadcastService):  # FIXME !!!!!!!
    def __init__(self, service):
        self.service = service

    def Broadcast(self, request, context):
        # TODO
        self.service.handle_broadcast(request.order_id, request.vector_clock)
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
