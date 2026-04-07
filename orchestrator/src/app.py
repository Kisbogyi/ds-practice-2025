import json
import logging
import asyncio
import os
import sys
import grpc.aio
from flask import Flask, request, jsonify
from flask_cors import CORS
import uuid
from typing import Any, Dict
import threading
from concurrent import futures

# pb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utils/pb'))
# for root, dirs, files in os.walk(pb_path):
#     sys.path.append(root)

import utils.pb.suggestions.suggestions_pb2 as suggestions
import utils.pb.suggestions.suggestions_pb2_grpc as suggestions_grpc

import utils.pb.fraud_detection.fraud_detection_pb2 as fraud_detection
import utils.pb.fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

import utils.pb.transaction_verification.transaction_verification_pb2 as transaction_verification
import utils.pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

import utils.pb.order_que.order_queue_pb2 as order_queue_pb2
import utils.pb.order_que.order_queue_pb2_grpc as order_queue_pb2_grpc

# TODO check if imports are correct
from utils.other.orderStateManager import OrderStateManager
from utils.other.orderResult import OrderResult
from utils.other.broadcast_service import broadcast_clear

logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="orchestrator")
order_results: Dict[str, OrderResult] = {}  # TODO locking

# ================================= grpc ================================= 

class TransactionVerificationServiceFinished(transaction_verification_grpc.TransactionVerificationServiceFinishedServicer):
    def Response(self, request, context):
        logger.info(f"Transaction status for {request.order_id} {request.success}")
        if request.success:
            order_results[request.order_id].pass_transaction({})
            #TODO DELETE FOLLOWING (JUST FOR TESTING PURPOSES):
            # vcs are placeholders
            order_results[request.order_id].pass_verefication({})
            order_results[request.order_id].set_suggestions({}, {})
        else:
            order_results[request.order_id].fail(Exception(request.reason))
        return transaction_verification.Empty()

async def transaction_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceInitStub(channel)
        result = await stub.InitOrder(transaction_verification.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            user_name=str(order_data.get("user_name", "")),
            card_number=str(order_data.get("card_number", "")),
            order_amount=int(order_data.get("order_amount", 0)),
            billing_address=str(order_data.get("billing_address", "")),
        ))
    return result.vc

async def transaction_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceInitStub(channel)
        result = await stub.ClearOrder(transaction_verification.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success

class FraudDetectionFinishHandler(fraud_detection_grpc.FraudDetectionServiceFinishedServicer):
    def Response(self, request, context):
        if request.success:
            order_results[request.order_id].pass_verefication()
        else:
            order_results[request.order_id].fail(Exception(request.reason))

# TODO: verification_init, suggestions_init


#TODO as TransactionVerificationServiceFinished
def set_suggestions(order_id: str, suggestions: Dict):
    order_results[order_id].set_suggestions(suggestions)


async def broadcast_init(order_id: str, trigger_vc: list[int], order_data: dict):
    logger.info(f"[BROADCAST INIT]: order {order_id}")
    # instead of broadcast do via grpc to configure triggers:
    tr_trigger_vc = await transaction_init(order_id, trigger_vc, order_data) # ex. trigger_vc: [1, 0, 0, 0]
    # fr_trigger_vc = await verification_init(order_id, trigger_vc, order_data)  # ex. trigger_vc: [1, 0, 0, 0]
    # s_trigger_vc = await suggestions_init(order_id, state_manager._merge_clocks(tr_trigger_vc, fr_trigger_vc), order_data)  # ex. trigger_vc: [1, 3, 2, 0]
    return tr_trigger_vc # TODO use: s_trigger_vc,  ex. trigger_vc: [1, 3, 2, 2]


async def broadcast_clear(order_id: str, final_vc: list[int]):
    logger.info(f"[BROADCAST CLEAR]: order {order_id}")
    results = await asyncio.gather(
        transaction_clear(order_id, final_vc),
        # verification_clear(order_id, trigger_vc),
        # suggestions_clear(order_id, trigger_vc),
    )
    return all(results)  # TODO

def enque_request(order_data) -> None:
    with grpc.insecure_channel('order_queue:50061') as channel:
        order_id = order_data["order_id"]
        stub = order_queue_pb2_grpc.OrderQueueServiceStub(channel)
        if stub.Enqueue(order_queue_pb2.EnqueueRequest(order_id=order_id)):
            logger.info(f"Succesfully enqued: {order_id}")
        else:
            logger.warning(f"Failed to enque: {order_id}")

# ================================= WEBSERVER =================================


# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})


@app.route('/checkout', methods=['POST'])
async def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # generate id and parse data
    try:
        request_data = json.loads(request.data)
    except Exception:
        logger.error(f"Invalid JSON")
        return jsonify({"error": "Order Rejected", "reason": "Invalid JSON"}), 400

    # initialize tracking
    result = OrderResult()
    order_id = str(uuid.uuid4())
    order_results[order_id] = result

    try:
        logger.info(f"Checkout started for order {order_id}")

        order_data = {
            "items": request_data.get('items', []),
            "user_name": request_data.get("user", {}).get("name"),
            "card_number": request_data.get("creditCard", {}).get("number"),
            "order_amount": request_data.get('order_amount', sum(item.get('quantity', 0) for item in request_data.get('items', []))),
            "billing_address": request_data.get("billingAddress", "")
        }

        # initiate distributed broadcast
        await state_manager.store_data(order_id, order_data)
        final_vc = await broadcast_init(order_id, await state_manager.get_final_vc(order_id, 1), order_data)
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
                    "status": "Order Accepted",
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
            success = await asyncio.wait_for(broadcast_clear(order_id, final_vc), timeout=10.0)
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

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_TransactionVerificationServiceFinishedServicer_to_server(
        TransactionVerificationServiceFinished(), server
    )
    port = "50059"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.debug(f"gRPC server started, listening on port {port}")
    server.wait_for_termination()

def run_flask():
    app.run(host='0.0.0.0')

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    # flask_thread = threading.Thread(target=run_flask, daemon=True)
    # flask_thread.start()
    # logger.info("Flask thread started...")
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    logger.info("gRPC thread started...")
    app.run(host='0.0.0.0')
