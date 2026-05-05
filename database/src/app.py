import logging
from concurrent import futures
import sys
import threading
import time
import grpc
import os

import utils.other.setup as setup
setup.initialize_pb_paths() # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.crud.crud_pb2 as books_pb2
import utils.pb.crud.crud_pb2_grpc as books_pb2_grpc

import utils.pb.commitment.commitment_pb2_grpc as commitment_pb2_grpc
import utils.pb.commitment.commitment_pb2 as commitment_pb2 

logger = setup.get_debug_logger(__name__)

class BooksDatabaseServicer(books_pb2_grpc.BookDatabaseServicer):
    store: dict[str, int]

    def __init__(self):
        self.store = {}

    def Read(self, request, context):
        stock: int = self.store.get(request.title, 0)
        logger.info(f"reading key: {request.title} value: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def ReadAll(self, request, context):    
        logger.info(f"reading all stock data {self.store}")
        return books_pb2.ReadAllResponse(stock_list=self.store)

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
        self.write_all(request.title, request.new_stock)
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

import grpc
import sys
import os

# Ensure the pb paths are loaded correctly
import utils.other.setup as setup
setup.initialize_pb_paths() # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.crud.crud_pb2 as books_pb2
import utils.pb.crud.crud_pb2_grpc as books_pb2_grpc

def populate_database():    
    dummy_books = {
        "Dune": 15,
        "1984": 10,
        "The Great Gatsby": 5,
        "Foundation": 20,
        "Neuromancer": 8,
        "Snow Crash": 12,
        "The Hobbit": 25,
        "Fahrenheit 451": 7,
        "Brave New World": 14,
        "The Martian": 30
    }
    print(f"Connecting to database...")
    try:
        with grpc.insecure_channel('db:50073') as channel:
            stub = books_pb2_grpc.BookDatabaseStub(channel)
            for title, stock in dummy_books.items():
                req = books_pb2.WriteRequest(title=title, new_stock=stock)
                response = stub.Write(req)
                if response.success:
                    print(f"Successfully added '{title}' (Stock: {stock})")
                else:
                    print(f"Failed to add '{title}'")
                    
        print("Database successfully populated!")
        
    except grpc.RpcError as e:
        print(f"gRPC Error: Could not connect to the database.")
        print(f"Details: {e.details()}")
        print("Make sure your database container is running and the port is accessible.")


if __name__ == "__main__":
    def delayed_populate():
        time.sleep(2)
        populate_database()
    threading.Thread(target=delayed_populate, daemon=True).start()
    start()