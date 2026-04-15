import logging
import sys
from goodreads import get_recommendations
import asyncio
import grpc.aio
import grpc

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.

from utils.other.orderStateManager import OrderStateManager
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2
import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.suggestions.suggestions_pb2 as suggestions
import utils.pb.suggestions.suggestions_pb2_grpc as suggestions_grpc


logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="suggestions_service")

class SuggestionsService(suggestions_grpc.SuggestionsServiceInitServicer):
    async def InitOrder(self, request, context):
        order_data = {
            "user_name": request.user_name,
            "order_amount": request.order_amount,
            "billing_address": request.billing_address,
            "card_number": request.card_number,
            "book_name": request.book_name,
        }
        logger.info(f"Init order {request.order_id}: {order_data}")
        await state_manager.store_data(request.order_id, order_data, request.vc)
        completionVC = await state_manager.get_final_vc(request.order_id, ticks=1)
        return suggestions.completionVC(vc=completionVC)

    async def ClearOrder(self, request, context):
        logger.info(f"Clear order: {request.order_id}")
        success = await state_manager.clear_data(request.order_id, request.vc)
        return suggestions.clearStatus(success=success)

    @staticmethod
    async def Response(order_id: str, success: bool, recommended_books: dict = {}) -> None:
        async with grpc.aio.insecure_channel('orchestrator:50051') as channel:
            stub = suggestions_grpc.SuggestionsServiceFinishedStub(channel)
            recommended_books = recommended_books or {"titles": [], "authors": [], "ids": []}
            _ = await stub.Response(suggestions.SuggestionResponse(
                order_id=order_id,
                success=success,
                titles=recommended_books.get("titles", []),
                authors=recommended_books.get("authors", []),
                id=recommended_books.get("ids", []),
            ))

    async def handle_broadcast(self, order_id: str, incoming_vc: list[int]):
        if await state_manager.is_vc_triggered(order_id, incoming_vc, 0):
            logger.info(f"Order {order_id} {incoming_vc}: Triggering Event F")
            asyncio.create_task(self.GenerateSuggestions(order_id, incoming_vc))

    # Event F
    async def GenerateSuggestions(self, order_id: str, incoming_vc: list[int]):
        order = await state_manager.get_data(order_id)
        book_name = order['book_name']
        
        rec = await asyncio.to_thread(get_recommendations, title=book_name)
        
        titles, authors, ids = [], [], []
        if rec:
            titles = [book.title for book in rec] 
            authors = [book.author for book in rec]
            ids = [book.id for book in rec]

        logger.info(f"Recommending the following books for {order_id}: {titles}") 

        suggested_books = {
            "titles": titles,
            "authors": authors,
            "ids": ids
        }

        await state_manager.process_event(order_id, incoming_vc)
        logger.info(f"VC after GenerateSuggestions for {order_id}: {await state_manager.get_vc(order_id)}")
        await self.Response(order_id, True, recommended_books=suggested_books)

    # def SuggestBook(self, request, context):
    #     # Create a HelloResponse object
    #     book_name = request.book_name
    #     logger.info(f"Book recommendation request arrived with: name {book_name}")
    #     response = suggestions.SuggestionResponse()
    #     rec = get_recommendations(title=book_name)
    #     if rec is None:
    #         titles = []
    #         authors = []
    #         ids = []
    #     else :
    #         # this is fine because it is max 5 books
    #         titles = [book.title for book in rec] 
    #         authors = [book.author for book in rec]
    #         ids = [book.id for book in rec]

    #     response.titles.extend(titles)
    #     response.authors.extend(authors)
    #     response.id.extend(ids)
    #     logger.info(f"Recommending the following books: {titles}") 
    #     # Set the greeting field of the response object
    #     return response

class BroadcastHandler(broadcast_grpc.BroadcastServiceServicer):
    def __init__(self, cls: SuggestionsService):
        self.cls = cls

    async def BroadcastVC(self, request, context):
        asyncio.create_task(self.cls.handle_broadcast(request.order_id, request.vector_clock))
        return broadcast_pb2.Empty()

async def serve():
    # Create a gRPC server
    server = grpc.aio.server()
    # Add HelloService
    service = SuggestionsService()
    suggestions_grpc.add_SuggestionsServiceInitServicer_to_server(service, server)
    # Listen on port 50053
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    broadcast_grpc.add_BroadcastServiceServicer_to_server(BroadcastHandler(service), server)
    port = "50054"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    await server.start()
    logger.debug("Server started. Listening on port 50053.")
    await server.wait_for_termination()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    asyncio.run(serve())

