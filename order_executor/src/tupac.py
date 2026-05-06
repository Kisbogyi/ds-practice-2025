from abc import ABC, abstractmethod
import grpc
import asyncio

import utils.other.setup as setup
setup.initialize_pb_paths()  # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.order_que.order_queue_pb2 as order_queue
import utils.pb.order_que.order_queue_pb2_grpc as order_queue_grpc
import utils.pb.commitment.commitment_pb2 as commitment_pb2
import utils.pb.commitment.commitment_pb2_grpc as commitment_pb2_grpc

logger = setup.get_debug_logger(__name__)


class CommitmentParticipant(ABC):
    @abstractmethod
    def Prepare(self, order_id: str, updated_stock: dict, payment_data: dict):
        raise NotImplementedError

    @abstractmethod
    def Commit(self, order_id: str, updated_stock: dict, payment_data: dict):
        raise NotImplementedError

    @abstractmethod
    def Abort(self, order_id: str, updated_stock: dict, payment_data: dict):
        raise NotImplementedError


class Payment(CommitmentParticipant):
    def Prepare(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('payment:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            logger.info("sending prepare message to db")
            _succeded = stub.Prepare(commitment_pb2.PrepareRequest(
                id=order_id, amount=0))  # FIXME
            return _succeded.ready

    def Commit(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('payment:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            logger.info(f"Sending commit messages for order {order_id}")
            for item_name in updated_stock.keys():
                response = stub.Commit(commitment_pb2.CommitRequest(
                    order_id=f"{order_id}/{item_name}",
                    title=item_name
                ))
                if not response.success:
                    return False
            return True

    def Abort(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('payment:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            _succeded = stub.Abort(commitment_pb2.AbortRequest(order_id=order_id))  # FIXME


class Database(CommitmentParticipant):
    def Prepare(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('db:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            logger.info(f"Sending prepare messages for order {order_id}")
            for item_name, updated_amount in updated_stock.items():
                response = stub.Prepare(commitment_pb2.PrepareRequest(
                    id=f"{order_id}/{item_name}",
                    amount=updated_amount
                ))
                if not response.ready:
                    return False
            return True

    def Commit(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('db:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            logger.info(f"Sending abort messages for order {order_id}")
            for item_name in updated_stock.keys():
                response = stub.Commit(commitment_pb2.CommitRequest(
                    order_id=f"{order_id}/{item_name}",
                    title=item_name
                ))
                if not response.success:
                    return False
            return True

    def Abort(self, order_id: str, updated_stock: dict, payment_data: dict):  # FIXME PROTO!
        with grpc.insecure_channel('db:50073') as channel:
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            logger.info(f"Sending prepare messages for order {order_id}")
            for item_name, updated_amount in updated_stock.items():
                response = stub.Abort(commitment_pb2.AbortRequest(
                    order_id=f"{order_id}/{item_name}",
                ))


def two_phase_commit(order_id: str, updated_stock: dict, payment_data: dict):
    participants: list[CommitmentParticipant] = [Database(), Payment()]
    ready_votes = []
    for service in participants:
        try:
            response = service.Prepare(order_id, updated_stock, payment_data)
            ready_votes.append(response)
        except Exception as ex:
            logger.warning(ex)
            ready_votes.append(False)
    logger.info(f"All services are ready {ready_votes}")

    if all(ready_votes):
        for service in participants:
            service.Commit(order_id, updated_stock, payment_data)
        logger.info("All services commited")
        asyncio.run(Response(order_id, True))
    else:
        for service in participants:
            service.Abort(order_id, updated_stock, payment_data)
        asyncio.run(Response(order_id, False, "Order commit rejected"))


async def Response(order_id: str, success: bool, reason: str = ""):
    async with grpc.aio.insecure_channel('orchestrator:50051') as channel:
        stub = order_queue_grpc.OrderQueueServiceFinishedStub(
            channel)
        _ = await stub.Response(order_queue.OrderResponse(
            order_id=order_id,
            success=success,
            reason=reason
        ))
