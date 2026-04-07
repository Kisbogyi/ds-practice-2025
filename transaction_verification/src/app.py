import asyncio
import grpc.aio
from concurrent import futures
import os
import grpc
import sys
import logging


pb_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../utils/pb'))
for root, dirs, files in os.walk(pb_path):
    sys.path.append(root)

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.

from utils.other.orderStateManager import OrderStateManager
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc
import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
# import utils.pb.broadcast.broadcast_pb2 as broadcast

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
    async def InitOrder(self, request, context):
        order_data = {
            "user_name": request.user_name,
            "order_amount": request.order_amount,
            "billing_address": request.billing_address,
            "card_number": request.card_number,
        }
        await state_manager.store_data(request.order_id, order_data, request.vc)
        # ticks = 3 - amount of events
        completionVC = await state_manager.get_final_vc(request.order_id, ticks=3)
        return transaction_verification.completionVC(vc=completionVC)

    async def ClearOrder(self, request, context):
        logger.info(f"Clear order: {request.order_id}")
        success = await state_manager.clear_data(request.order_id, request.vc)
        return transaction_verification.clearStatus(success=success)

    @staticmethod
    async def Response(order_id: str, success: bool, reason: str = "") -> None:
        """ Call this when transaction verification is finished
        This will send a grpc request to the orchestrator.

        args:
            failed: if the transaction verification failed becaouse of an unknown error
            is_valid: if the transaction is valid or not
        """
        async with grpc.aio.insecure_channel('orchestrator:50051') as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceFinishedStub(
                channel)
            _ = await stub.Response(transaction_verification.VerificationResponse(
                order_id=order_id,
                success=success,
                reason=reason
            ))

    async def handle_broadcast(self, order_id: str, incoming_vc: list[int]):
        if await state_manager.is_vc_triggered(order_id, incoming_vc, 0):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Event A and Event B")
            asyncio.create_task(self.VerifyItems(order_id, incoming_vc))     # Event A
            asyncio.create_task(self.VerifyUserData(order_id, incoming_vc))  # Event B

        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 2):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Event C")
            asyncio.create_task(self.VerifyItems(order_id, incoming_vc))     # Event C

        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 3):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Final Response")
            asyncio.create_task(self.Response(order_id, True))


    # Event (a):
    async def VerifyItems(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        order_amount = order["order_amount"]

        is_valid = order_amount > 0 # FIXME to actual item check

        if not is_valid:
            logger.warning(f"Order {order_id}: Order has no items")
            return await self.Response(order_id, False, "Order has no items")

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(
            f"VC after VerifyItems for {order_id}: {await state_manager.get_vc(order_id)}")

    # Event (b):
    async def VerifyUserData(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        user_name = order.get("user_name", "")

        is_valid = len(user_name) > 0

        if not is_valid:
            logger.warning(f"Order {order_id}: Invalid username")
            return await self.Response(order_id, False, "Invalid username")

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
            logger.warning(f"Order {order_id}: {reason}")
            return await self.Response(order_id, False, reason)

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(
            f"VC after VerifyCreditCard for {order_id}: {await state_manager.get_vc(order_id)}")


class BroadcastHandler(broadcast_grpc.BroadcastServiceServicer):
    def __init__(self, cls: TransactionVerificationService):
        self.cls = cls

    async def BroadcastVC(self, request, context):
        await self.cls.handle_broadcast(request.order_id, request.vector_clock)
        return broadcast_pb2.Empty()


# class BroadcastClearHandler(broadcast_grpc.BroadcastClearServicer):
#     def __init__(self, cls: TransactionVerificationService):
#         self.cls = cls

#     def BroadcastService(self, request, context):
#         order_id: str = request.order_id
#         vc: list[int] = request.vector_clock
#         self.cls.clear_order(order_id, vc)
#         return broadcast_pb2.Empty()


async def serve():
    # server = grpc.server(futures.ThreadPoolExecutor())
    server = grpc.aio.server(futures.ThreadPoolExecutor())

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
    # broadcast_grpc.add_BroadcastClearServicer_to_server(
    #     BroadcastClearHandler(service), server
    # )
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    await server.start()
    logger.debug(f"Server started. Listening on port {port}.")
    await server.wait_for_termination()


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # serve()
    asyncio.run(serve())
