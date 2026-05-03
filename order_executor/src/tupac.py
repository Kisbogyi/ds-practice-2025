from abc import ABC, abstractmethod
import grpc
import logging
import sys

import commitment.commitment_pb2_grpc as commitment_pb2_grpc
import commitment.commitment_pb2 as commitment_pb2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class CommitmentParticipant(ABC):
    @abstractmethod
    def Prepare(self, id: str, amount: int = 0):
        raise NotImplementedError

    @abstractmethod
    def Commit(self, order_id: str):
        raise NotImplementedError

    @abstractmethod
    def Abort(self, order_id: str):
        raise NotImplementedError

class Payment(CommitmentParticipant):
    def Prepare(self, id: str, amount: int = 0):
        with grpc.insecure_channel('payment:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            logger.info("sending prepare message to payment")
            _succeded = stub.Prepare(commitment_pb2.PrepareRequest(id=id, amount=amount))

    def Commit(self, order_id: str):
        with grpc.insecure_channel('payment:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            _succeded = stub.Commit(commitment_pb2.CommitRequest(order_id=order_id, title=""))

    def Abort(self, order_id: str):
        with grpc.insecure_channel('payment:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            _succeded = stub.Abort(commitment_pb2.AbortRequest(order_id=str(order_id))) 

class Database(CommitmentParticipant):
    def Prepare(self, id: str, amount: int = 0):
        with grpc.insecure_channel('db:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            logger.info("sending prepare message to db")
            _succeded = stub.Prepare(commitment_pb2.PrepareRequest(id=id, amount=amount))

    def Commit(self, order_id: str):
        with grpc.insecure_channel('db:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            _succeded = stub.Commit(commitment_pb2.CommitRequest(order_id=order_id, title=""))

    def Abort(self, order_id: str):
        with grpc.insecure_channel('db:50073') as channel:
            # Create a stub object.
            stub = commitment_pb2_grpc.CommitmentSchemeStub(channel=channel)
            # Call the service through the stub object.
            _succeded = stub.Abort(commitment_pb2.AbortRequest(order_id=order_id)) 


def two_phase_commit(order_id: str, amount: int):
    participants: list[CommitmentParticipant] = [Database(), Payment()]
    ready_votes = []
    for service in participants:
        try:
            response = service.Prepare(id=str(order_id), amount=amount)
            ready_votes.append(response.ready)
        except Exception:
            ready_votes.append(False)
    print("All services are ready")
    
    if all(ready_votes):
        for service in participants:
            service.Commit(order_id=str(order_id))
        print("All services commited")
    else:
        for service in participants:
            service.Abort(order_id=str(order_id))
