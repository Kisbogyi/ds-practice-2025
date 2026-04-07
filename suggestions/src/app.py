import logging
import sys
import os
from goodreads import get_recommendations
import utils.pb.broadcast.broadcast_pb2 as broadcast
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc
from concurrent import futures

logger = logging.getLogger(__name__)

# Create a class to define the server functions, derived from
# suggestions_pb2_grpc.HelloServiceServicer
class SuggestionsService(suggestions_grpc.SuggestionsService):
    # Create an RPC function to say hello
    def SuggestBook(self, request, context):
        # Create a HelloResponse object
        book_name = request.book_name
        logger.info(f"Book recommendation request arrived with: name {book_name}")
        response = suggestions.SuggestionResponse()
        rec = get_recommendations(title=book_name)
        if rec is None:
            titles = []
            authors = []
            ids = []
        else :
            # this is fine because it is max 5 books
            titles = [book.title for book in rec] 
            authors = [book.author for book in rec]
            ids = [book.id for book in rec]

        response.titles.extend(titles)
        response.authors.extend(authors)
        response.id.extend(ids)
        logger.info(f"Recommending the following books: {titles}") 
        # Set the greeting field of the response object
        return response

class BroadcastService(broadcast_grpc.BroadcastService):
    def Broadcast(self, request, context):
        #call function 
        return 

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add HelloService
    suggestions_grpc.add_SuggestionsServiceServicer_to_server(SuggestionsService(), server)
    # Listen on port 50053
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastService(), server)
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.debug("Server started. Listening on port 50053.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    serve()
