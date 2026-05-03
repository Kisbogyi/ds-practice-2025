import grpc

import crud.crud_pb2 as books_pb2
import crud.crud_pb2_grpc as books_pb2_grpc

def write(key: str, value: int):
    """Writes the value to the db with the key
    """

    with grpc.insecure_channel('database_master:50071') as channel:
        # Create a stub object.
        stub = books_pb2_grpc.BookDatabaseStub(channel=channel)
        # Call the service through the stub object.
        _succeded = stub.Write(books_pb2.WriteRequest(title=key, new_stock=value))

