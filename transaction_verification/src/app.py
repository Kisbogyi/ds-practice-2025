import asyncio
from concurrent import futures
import grpc
import sys
import logging

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

# import utils.pb.broadcast.broadcast_pb2 as broadcast
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2

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


class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationServiceInitServicer):
    async def CheckFraud(self, request, context):
        order_id: str = request.order_id
        trigger_vc: list[int] = request.vc
        order_data = {
            "username": request.username,
            "order_amount": request.order_amount,
            "billing_address": request.billing_address
        }
        completionVC = await self.init_order(order_id, trigger_vc, order_data)
        response = transaction_verification.completionVC(vc=completionVC)
        return response

    @staticmethod
    async def Response(order_id: str, success: bool, reason: str = "") -> None:
        """ Call this when transaction verification is finished
        This will send a grpc request to the orchestrator.
        
        args: 
            failed: if the transaction verification failed becaouse of an unknown error
            is_valid: if the transaction is valid or not
        """
        async with grpc.aio.insecure_channel('orchestrator:50051') as channel:
            # Create a stub object.
            stub = transaction_verification_grpc.TransactionVerificationServiceFinishedStub(channel)
            # Call the service through the stub object.
            _ = await stub.Response(transaction_verification.VerificationResponse(
                order_id=order_id,
                success=success,
                reason=reason
            ))

    async def init_order(self, order_id: str, trigger_vc: list[int], order_data: dict) -> dict:
        await state_manager.store_data(order_id, order_data, trigger_vc)
        return state_manager.get_final_vc(order_id, 3)

    async def clear_order(self, order_id: str, incoming_vc: list[int]):
        return await state_manager.clear_data(order_id, incoming_vc)

    # TODO add async or threads
    async def handle_broadcast(self, order_id: str, incoming_vc: dict):
        vc = (await state_manager.get_data(order_id, incoming_vc))[VECTOR_CLOCK]
        # entry point
        if await state_manager.match_target_vc(order_id, vc) and vc['verification_service'] == 0:
            asyncio.gather(
                self.VerifyItems(order_id, vc),  # event A
                self.VerifyUserData(order_id, vc)  # event B
            )
        elif vc['verification_service'] == 2:
            self.VerifyItems(order_id, vc)  # event C
        elif vc['verification_service'] == 3:  # if all passed -> status is valid
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


class BroadcastHandler(broadcast_grpc.BroadcastServiceServicer):
    def  __init__(self, cls: TransactionVerificationService):
        self.cls = cls

    def BroadcastService(self, request, context):
        order_id: str = request.order_id
        vc: list[int] = request.vector_clock
        self.cls.handle_broadcast(order_id, vc)
        return broadcast_pb2.Empty()

class BroadcastClearHandler(broadcast_grpc.BroadcastClearServicer):
    def  __init__(self, cls: TransactionVerificationService):
        self.cls = cls

    def BroadcastService(self, request, context):
        order_id: str = request.order_id
        vc: list[int] = request.vector_clock
        self.cls.clear_order(order_id, vc)
        return broadcast_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    service = TransactionVerificationService()
    transaction_verification_grpc.add_TransactionVerificationServiceInitServicer_to_server(
        service, server
    )
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # 2 endpoints?

    broadcast_grpc.add_BroadcastServiceServicer_to_server(
        BroadcastHandler(service), server
    )
    broadcast_grpc.add_BroadcastClearServicer_to_server(
        BroadcastClearHandler(service), server
    )
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
