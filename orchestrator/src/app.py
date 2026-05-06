
import json
import logging
import asyncio
import os
import sys
import grpc.aio
from quart import Quart, request, jsonify
from quart_cors import cors
import uuid
from typing import Dict

import utils.other.setup as setup
setup.initialize_pb_paths()  # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!


# TODO check if imports are correct
from utils.other.orderResult import OrderResult
from utils.other.orderStateManager import OrderStateManager
import utils.pb.crud.crud_pb2_grpc as books_pb2_grpc
import utils.pb.crud.crud_pb2 as books_pb2
import utils.pb.order_que.order_queue_pb2_grpc as order_queue_pb2_grpc
import utils.pb.order_que.order_queue_pb2 as order_queue_pb2
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc
import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
import utils.pb.fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc
import utils.pb.fraud_detection.fraud_detection_pb2 as fraud_detection
import utils.pb.suggestions.suggestions_pb2_grpc as suggestions_grpc
import utils.pb.suggestions.suggestions_pb2 as suggestions
import utils.pb.order_que.order_queue_pb2_grpc as order_queue_grpc
import utils.pb.order_que.order_queue_pb2 as order_queue


logger = setup.getLogger(__name__)
state_manager = OrderStateManager(service_name="orchestrator")
order_results: Dict[str, OrderResult] = {}  # TODO locking

# ================================= grpc =================================


class TransactionVerificationServiceFinished(transaction_verification_grpc.TransactionVerificationServiceFinishedServicer):
    async def Response(self, request, context):
        logger.info(
            f"Transaction status for {request.order_id} {request.success}")

        if request.order_id not in order_results:
            logger.warning(
                f"Response received for unknown order: {request.order_id}")
            return transaction_verification.Empty()

        if request.success:
            order_results[request.order_id].pass_transaction()
        else:
            order_results[request.order_id].fail(Exception(request.reason))
        return transaction_verification.Empty()


class FraudDetectionServiceFinishHandler(fraud_detection_grpc.FraudDetectionServiceFinishedServicer):
    def Response(self, request, context):
        logger.info(f"Fraud status for {request.order_id} {request.success}")

        if request.order_id not in order_results:
            logger.warning(
                f"Response received for unknown order: {request.order_id}")
            return transaction_verification.Empty()

        if request.success:
            order_results[request.order_id].pass_verefication()
        else:
            order_results[request.order_id].fail(Exception(request.reason))

        return transaction_verification.Empty()


class SuggestionsServiceFinishedHandler(suggestions_grpc.SuggestionsServiceFinishedServicer):
    async def Response(self, request, context):
        logger.info(f"Suggestions arrived for order {request.order_id}")

        if request.order_id not in order_results:
            logger.info(
                f"Response received for unknown order: {request.order_id} {request.success}")
            return suggestions.Empty()

        if request.success:
            formatted_suggestions = []
            for i in range(len(request.titles)):
                formatted_suggestions.append({
                    "title": request.titles[i],
                    "author": request.authors[i],
                    "id": request.id[i]
                })
            order_results[request.order_id].set_suggestions(
                formatted_suggestions)
        else:
            order_results[request.order_id].fail(Exception(request.reason))

        return suggestions.Empty()

class OrderServiceFinishedHandler(order_queue_grpc.OrderQueueServiceFinishedServicer):
    def Response(self, request, context):
        logger.info(f"Order status for {request.order_id} {request.success}")
        if request.order_id not in order_results:
            logger.warning(
                f"Response received for unknown order: {request.order_id}")
            return order_queue.Empty()

        if request.success:
            order_results[request.order_id].complete_order()
        else:
            order_results[request.order_id].fail(Exception(request.reason))
        return order_queue.Empty()


async def transaction_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceInitStub(
            channel)
        result = await stub.InitOrder(transaction_verification.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            user_name=str(order_data.get("user_name", "")),
            card_number=str(order_data.get("card_number", "")),
            billing_address=str(order_data.get("billing_address", "")),
            order=dict(order_data.get("order", {}))
        ))
    return result.vc


async def verification_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceInitStub(channel)
        result = await stub.InitOrder(fraud_detection.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            user_name=str(order_data.get("user_name", "")),
            card_number=str(order_data.get("card_number", "")),
            billing_address=str(order_data.get("billing_address", "")),
            order=dict(order_data.get("order", {}))
        ))
    return result.vc

async def suggestion_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsServiceInitStub(channel)
        result = await stub.InitOrder(suggestions.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            user_name=str(order_data.get("user_name", "")),
            card_number=str(order_data.get("card_number", "")),
            billing_address=str(order_data.get("billing_address", "")),
            order=dict(order_data.get("order", {}))
        ))
    return result.vc


async def order_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('order_queue:50061') as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        result = await stub.InitOrder(order_queue.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            user_name=str(order_data.get("user_name", "")),
            card_number=str(order_data.get("card_number", "")),
            billing_address=str(order_data.get("billing_address", "")),
            order=dict(order_data.get("order", {}))
        ))
    return result.vc


async def transaction_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceInitStub(
            channel)
        result = await stub.ClearOrder(transaction_verification.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success


async def verification_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceInitStub(channel)
        result = await stub.ClearOrder(fraud_detection.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success


async def suggestions_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsServiceInitStub(channel)
        result = await stub.ClearOrder(suggestions.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success

async def order_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('order_queue:50061') as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        result = await stub.ClearOrder(order_queue.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success

# TODO as TransactionVerificationServiceFinished
def set_suggestions(order_id: str, suggestions: list):
    order_results[order_id].set_suggestions(suggestions)


async def broadcast_init(order_id: str, trigger_vc: list[int], order_data: dict):
    logger.info(f"[BROADCAST INIT]: order {order_id}")
    tr_task = transaction_init(order_id, trigger_vc, order_data)
    fr_task = verification_init(order_id, trigger_vc, order_data)

    tr_trigger_vc, fr_trigger_vc = await asyncio.gather(tr_task, fr_task)
    merged_vc = state_manager.merge_clocks(tr_trigger_vc, fr_trigger_vc)
    logger.info(f"Stage 2 vc {merged_vc}")

    s_task = suggestion_init(order_id, merged_vc, order_data)
    o_task = order_init(order_id, merged_vc, order_data)

    s_trigger_vc, o_trigger_vc = await asyncio.gather(s_task, o_task)
    expected_vc = state_manager.merge_clocks(s_trigger_vc, o_trigger_vc)

    logger.info(f"Epected vc: {expected_vc}")
    return expected_vc  # ex. final vc: [1, 3, 2, 1, 1]


async def broadcast_clear_vc(order_id: str, final_vc: list[int]):
    logger.info(f"[BROADCAST CLEAR]: order {order_id}")
    results = await asyncio.gather(
        transaction_clear(order_id, final_vc),
        verification_clear(order_id, final_vc),
        suggestions_clear(order_id, final_vc),
        order_clear(order_id, final_vc)
    )
    return all(results)  # TODO

# def enque_request(order_id: str) -> None:
#     with grpc.insecure_channel('order_queue:50061') as channel:
#         stub = order_queue_pb2_grpc.OrderQueueServiceStub(channel)
#         if stub.Enqueue(order_queue_pb2.EnqueueRequest(order_id=order_id)):
#             logger.info(f"Succesfully enqued: {order_id}")
#         else:
#             logger.warning(f"Failed to enque: {order_id}")


async def fetch_stock_from_db(book_title: str) -> int:
    async with grpc.aio.insecure_channel('db:50073') as channel:
        stub = books_pb2_grpc.BookDatabaseStub(channel=channel)
        try:
            req = books_pb2.ReadRequest(title=book_title)
            response = await stub.Read(req)
            return response.stock
        except grpc.RpcError as e:
            logger.error(f"Failed to fetch stock for '{book_title}': {e}")
            return -1  # error state


async def fetch_all_stock_from_db() -> dict:
    async with grpc.aio.insecure_channel('db:50073') as channel:
        stub = books_pb2_grpc.BookDatabaseStub(channel)
        try:
            req = books_pb2.ReadAllRequest()
            response = await stub.ReadAll(req)
            return dict(response.stock_list)
        except grpc.RpcError as e:
            logger.error(f"Failed to fetch all stock: {e}")
            return None

# ================================= WEBSERVER =================================


# Create a simple Quart app.
app = Quart(__name__)
# Enable CORS for the app.
cors(app, allow_origin="*")  # resources={r'/*': {'origins': '*'}})


@app.route('/stock', methods=['GET'])
async def get_all_stock():
    logger.info("Received request for all stock")
    stock_data = await fetch_all_stock_from_db()
    if stock_data is None:
        return jsonify({"error": "Failed to communicate with Database"}), 500
    return jsonify({
        "total_items": len(stock_data),
        "stock": stock_data
    }), 200


@app.route('/stock/<string:book_title>', methods=['GET'])
async def get_stock(book_title):
    logger.info(f"Received stock request for book: {book_title}")
    stock_amount = await fetch_stock_from_db(book_title)
    if stock_amount == -1:
        return jsonify({"error": "Failed to communicate with Database", "title": book_title}), 500
    return jsonify({
        "title": book_title,
        "stock": stock_amount
    }), 200


@app.route('/checkout', methods=['POST'])
async def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # generate id and parse data
    try:
        # request_data = json.loads(request.data)
        request_data = await request.get_json(force=True)
    except Exception:
        logger.error("Invalid JSON")
        return jsonify({"error": "Order Rejected", "reason": "Invalid JSON"}), 400

    # initialize tracking
    result = OrderResult()
    order_id = str(uuid.uuid4())
    order_results[order_id] = result

    try:

        order_data = {
            "order_id": order_id,
            "user_name": request_data.get("user", {}).get("name"),
            "card_number": request_data.get("creditCard", {}).get("number"),
            "billing_address": request_data.get("billingAddress", ""),
            "order": {item.get('name', ''): item.get('quantity', 1) for item in request_data.get('items', [])},
        }
        logger.info(f"Checkout started for order {order_id}: {order_data}")

        # initiate distributed broadcast
        await state_manager.store_data(order_id, order_data)

        start_vc = await state_manager.get_final_vc(order_id, 1)
        final_vc = await broadcast_init(order_id, start_vc, order_data)
        await state_manager.process_event(order_id)

        # handle order processing completion
        try:
            await asyncio.wait_for(result.wait(), timeout=10.0)
            if result.has_errors():
                logger.warning(f"Order {order_id}: {result.error}")
                status_data = {
                    "orderId": order_id,
                    "status": "Order Rejected",
                    "suggestedBooks": [],
                    "reason": str(result.error)
                }
            else:
                status_data = {
                    "orderId": order_id,
                    "status": "Order Approved",
                    "suggestedBooks": result.suggestions
                }
        except asyncio.TimeoutError:
            logger.error(f"Order {order_id}: Processing timeout")
            status_data = {
                "orderId": order_id,
                "status": "Order Rejected",
                "reason": "Processing timeout"
            }

        # cleanup broadcast
        try:
            success = await asyncio.wait_for(broadcast_clear_vc(order_id, final_vc), timeout=10.0)
            if not success:
                logger.error(f"Order {order_id}: Inconsistent Vector Clocks")
                status_data["status"] = "Order Rejected"
                status_data["reason"] = "Inconsistent Vector Clocks"
        except asyncio.TimeoutError:
            logger.error(f"Order {order_id}: Cleanup timeout")
            status_data["status"] = "Order Rejected"
            status_data["reason"] = "Cleanup timeout"

    finally:
        # finish cleanup
        order_results.pop(order_id, None)
        await state_manager.clear_data(order_id)

    response = json.dumps(status_data)
    logger.info(f"Response for {order_id}: {response}")
    return response


# ================================= gRPC Server Lifecycle =================================

grpc_server = None


@app.before_serving
async def start_grpc_server():
    global grpc_server
    grpc_server = grpc.aio.server()
    transaction_verification_grpc.add_TransactionVerificationServiceFinishedServicer_to_server(
        TransactionVerificationServiceFinished(), grpc_server
    )
    fraud_detection_grpc.add_FraudDetectionServiceFinishedServicer_to_server(
        FraudDetectionServiceFinishHandler(), grpc_server
    )
    suggestions_grpc.add_SuggestionsServiceFinishedServicer_to_server(
        SuggestionsServiceFinishedHandler(), grpc_server
    )
    order_queue_grpc.add_OrderQueueServiceFinishedServicer_to_server(
        OrderServiceFinishedHandler(), grpc_server
    )
    port = "50051"
    grpc_server.add_insecure_port(f"[::]:{port}")
    await grpc_server.start()
    logger.info(f"gRPC Server started, listening on {port}")


@app.after_serving
async def stop_grpc_server():
    """Gracefully shuts down the gRPC server when Quart stops."""
    global grpc_server
    if grpc_server:
        await grpc_server.stop(grace=2)


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Quart's app.run handles all the asyncio loop creation automatically.
    # We no longer need threads or custom loops!
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
