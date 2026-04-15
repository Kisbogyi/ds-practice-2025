import sys
import logging
import time
import grpc
import grpc.aio
import asyncio

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
from utils.other.orderStateManager import OrderStateManager
import utils.pb.fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc
import utils.pb.fraud_detection.fraud_detection_pb2 as fraud_detection
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2

logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="fraud_detection_service")

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
    risk: float = .0
    risk += transaction_amount * 0.5

    now = time.time()
    last_week = now - (7 * 24 * 60 * 60)
    transactions_last_week = len(list(filter(
        lambda transaction_timestamp: transaction_timestamp > last_week, transaction_history)))
    risk += transactions_last_week * 5

    if len(transaction_history) == 0:
        risk += 25

    # Dummy value should need all European union country names
    if "USA" not in billing_address or "EU" not in billing_address:
        risk += 25
    return risk


def check_card_blacklist(card_number):
    return False  # TODO


def check_card_velocity(card_number):
    return False  # TODO


class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceInitServicer):

    async def InitOrder(self, request, context):
        order_data = {
            "user_name": request.user_name,
            "order_amount": request.order_amount,
            "billing_address": request.billing_address,
            "card_number": request.card_number,
        }
        logger.info(f"Init order {request.order_id}: {order_data}")
        await state_manager.store_data(request.order_id, order_data, request.vc)
        completionVC = await state_manager.get_final_vc(request.order_id, ticks=2)
        return fraud_detection.completionVC(vc=completionVC)

    async def ClearOrder(self, request, context):
        logger.info(f"Clear order: {request.order_id}")
        success = await state_manager.clear_data(request.order_id, request.vc)
        return fraud_detection.clearStatus(success=success)

    @staticmethod
    async def Response(order_id: str, success: bool, reason: str = "") -> None:
        async with grpc.aio.insecure_channel('orchestrator:50051') as channel:
            stub = fraud_detection_grpc.FraudDetectionServiceFinishedStub(
                channel)
            _ = await stub.Response(fraud_detection.FraudResponse(
                order_id=order_id,
                success=success,
                reason=reason
            ))

    async def handle_broadcast(self, order_id: str, incoming_vc: list[int]):
        if await state_manager.is_vc_triggered(order_id, incoming_vc, 0):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Event D")
            asyncio.create_task(self.VerifyUserData(order_id, incoming_vc))  # Event D TODO

        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 1):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Event E")
            asyncio.create_task(self.VerifyCreditCard(order_id, incoming_vc))  # Event E TODO

        elif await state_manager.is_vc_triggered(order_id, incoming_vc, 2):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Final Response")
            asyncio.create_task(self.Response(order_id, True))

    # Event (d):
    async def VerifyUserData(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        username = order.get("user_name", "")
        order_amount = order.get("order_amount", 0)
        billing_address = order.get("billing_address", "")

        logger.info(
            f"Fraud check (User Data) for {order_id}: username: {username}, amount: {order_amount}")

        transaction_history = get_transaction_history(username)
        risk = await asyncio.to_thread(calculate_risk, username, order_amount, billing_address, transaction_history)

        logger.info(f"User Data Risk Score for {order_id}: {risk}")
        is_fraud = risk >= 55

        if is_fraud:
            logger.warning(
                f"Order {order_id}: Rejected due to high user fraud risk.")
            return await self.Response(order_id, False, "User flagged for high fraud risk")

        add_to_transaction_log(username)

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(f"VC after VerifyUserData for {order_id}: {await state_manager.get_vc(order_id)}")

    # Event (e):
    async def VerifyCreditCard(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        card_number = order.get("card_number", "")

        logger.info(
            f"Fraud check (Credit Card) for {order_id}: card ending in {card_number[-4:] if card_number else 'UNKNOWN'}")

        is_blacklisted = check_card_blacklist(card_number)

        if is_blacklisted:
            logger.warning(f"Order {order_id}: Credit card is blacklisted.")
            return await self.Response(order_id, False, "Credit card is blacklisted")

        velocity_flag = check_card_velocity(card_number)
        if velocity_flag:
            logger.warning(
                f"Order {order_id}: Unusual transaction volume on this credit card.")
            return await self.Response(order_id, False, "Card flagged for unusual activity")

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(f"VC after VerifyCreditCard for {order_id}: {await state_manager.get_vc(order_id)}")


class BroadcastService(broadcast_grpc.BroadcastServiceServicer):
    def __init__(self, cls: FraudDetectionService):
        self.cls = cls

    async def BroadcastVC(self, request, context): 
        asyncio.create_task(self.cls.handle_broadcast(
            request.order_id, request.vector_clock))
        return broadcast_pb2.Empty()


async def serve():
    server = grpc.aio.server()

    service = FraudDetectionService()
    fraud_detection_grpc.add_FraudDetectionServiceInitServicer_to_server(
        service, server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)

    broadcast_grpc.add_BroadcastServiceServicer_to_server(
        BroadcastService(service), server
    )
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    await server.start()
    logger.info("Server started. Listening on port 50051.")
    await server.wait_for_termination()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    asyncio.run(serve())
