import logging
from concurrent import futures
import sys
import grpc
import os

import crud.crud_pb2 as books_pb2
import crud.crud_pb2_grpc as books_pb2_grpc

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
        return books_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        self.store[request.title] = request.new_stock
        return books_pb2.WriteResponse(success=True)

class PrimaryReplica(BooksDatabaseServicer):
    backups: list[BooksDatabaseServicer]

    def __init__(self, backup_stubs):
        super().__init__()
        self.backups = backup_stubs

    def Write(self, request, context):
        self.store[request.title] = request.new_stock

        for backup in self.backups:
            try:
                backup.Write(request, None)
            except Exception as e:
                logger.error(f"Failed to replicate to backup: {e}")
        return books_pb2.WriteResponse(success=True)

def start():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())

    backup_stubs = ["db_backup_1", "db_backup_2"]
    if os.getenv("PRIMARY", ""):
        service = PrimaryReplica(backup_stubs)
    else: 
        service = BooksDatabaseServicer()

    books_pb2_grpc.add_BookDatabaseServicer_to_server(
        service, server
    )

    port = "50071"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == "__main__":
    logger.info("service started")
