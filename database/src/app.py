import logging
from concurrent import futures
import sys
import grpc
import os

import crud.crud_pb2 as books_pb2
import crud.crud_pb2_grpc as books_pb2_grpc

import commitment.commitment_pb2_grpc as commitment_pb2_grpc
import commitment.commitment_pb2 as commitment_pb2 

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class BooksDatabaseServicer(books_pb2_grpc.BookDatabaseServicer):
    store: dict[str, int]

    def __init__(self):
        self.store = {}

    def Read(self, request, context):
        stock: int = self.store.get(request.title, 0)
        logger.info(f"reading key: {request.title} value: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        self.store[request.title] = request.new_stock
        return books_pb2.WriteResponse(success=True)

    def write(self, id: str, new_stock: int):
        self.store[id] = new_stock 


class PrimaryReplica(BooksDatabaseServicer):
    backups: list[BooksDatabaseServicer]

    def __init__(self, backup_stubs):
        super().__init__()
        self.backups = backup_stubs

    def Write(self, request, context):
        return books_pb2.WriteResponse(success=True)

    def write_all(self, id: str, new_stock: int):
        self.store[id] = new_stock

        for backup in self.backups:
            try:
                backup.write(id, new_stock)
            except Exception as e:
                logger.error(f"Failed to replicate to backup: {e}")

class DatabaseParticipant(commitment_pb2_grpc.CommitmentSchemeServicer):
    def __init__(self, db: PrimaryReplica):
        self.temp_updates = {}
        self.db = db

    def Prepare(self, request, context):
        logger.info(f"preparing request with id {request.id} and stock {request.amount}")
        self.temp_updates[request.id] = request.amount
        return commitment_pb2.PrepareResponse(ready=True)
    
    def Commit(self, request, context):
        logger.info("Commit message came")
        update = self.temp_updates.pop(request.order_id, None)
        if update:
            self.db.store[request.title] = update
        return commitment_pb2.CommitResponse(success=True)

    def Abort(self, request, context):
        logger.info("Abort message came")
        self.temp_updates.pop(request.order_id, None)
        return commitment_pb2.AbortResponse(aborted=True)

def start():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())

    # backup_stubs = ["database_slave_1", "database_slave_22"]
    # if os.getenv("PRIMARY", ""):
    #     service = PrimaryReplica(backup_stubs)
    #     logger.info("Initialized as Master")
    # else: 
    #     service = BooksDatabaseServicer()
    #     logger.info("Initialized as Slave")

    service = PrimaryReplica([BooksDatabaseServicer(), BooksDatabaseServicer()])
    books_pb2_grpc.add_BookDatabaseServicer_to_server(
        service, server
    )
    commitment_pb2_grpc.add_CommitmentSchemeServicer_to_server(DatabaseParticipant(service), server)

    port = "50073"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == "__main__":
    start()
    logger.info("service started")
