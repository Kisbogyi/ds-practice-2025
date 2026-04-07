import logging
import asyncio
import sys
import grpc.aio
from flask import Flask, request, jsonify
from flask_cors import CORS
import uuid
from typing import Any, Dict

import suggestions.suggestions_pb2 as suggestions
import suggestions.suggestions_pb2_grpc as suggestions_grpc

import fraud_detection.fraud_detection_pb2 as fraud_detection
import fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

import transaction_verification.transaction_verification_pb2 as transaction_verification
import transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

# TODO check if imports are correct
from utils.other.orderStateManager import OrderStateManager
from utils.other.orderResult import OrderResult
from utils.other.broadcast import broadcast_clear

logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="orchestrator")
order_results: Dict[str, OrderResult] = {}  # TODO locking

# ================================= grpc ================================= 

class TransactionVerificationServiceFinished(transaction_verification_grpc.TransactionVerificationServiceFinishedServicer):
    def Response(self, response, context):
        if response.success:
            order_results[response.order_id].pass_transaction()
            #TODO DELETE FOLLOWING (JUST FOR TESTING PURPOSES):
            order_results[response.order_id].pass_verefication()
            order_results[response.order_id].set_suggestions([])
        else:
            order_results[response.order_id].fail(Exception(response.reason))

async def transaction_init(order_id: str, trigger_vc: list[int], order_data: dict) -> list[int]:
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceInitStub(channel)
        result = await stub.InitOrder(fraud_detection.InitRequest(
            order_id=order_id,
            vc=trigger_vc,
            username=order_data["username"],
            order_amount=order_data["order_amount"],
            billing_address=order_data["billing_address"],
        ))
    return result.vc

async def transaction_clear(order_id: str, final_vc: list[int]) -> bool:
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceInitStub(channel)
        result = await stub.ClearOrder(fraud_detection.ClearRequest(
            order_id=order_id,
            vc=final_vc,
        ))
    return result.success

class FraudDetectionFinishHandler(fraud_detection_grpc.FraudDetectionServiceFinishedServicer):
    def Response(self, response, context):
        if response.success:
            order_results[response.order_id].pass_verefication()
        else:
            order_results[response.order_id].fail(Exception(response.reason))

# TODO: verification_init, suggestions_init


#TODO as TransactionVerificationServiceFinished
def set_suggestions(order_id: str, suggestions: Dict):
    order_results[order_id].set_suggestions(suggestions)


async def broadcast_init(order_id: str, trigger_vc: dict, order_data: dict):
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
    order_id = str(uuid.uuid4())
    try:
        request_data = await request.get_json()
    except Exception:
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
            "order_amount": len(request_data.get('items', [])),
            "billing_address": request_data.get("billingAddress")
        }

        # initiate distributed broadcast
        await state_manager.store_data(order_id, order_data)
        final_vc = await broadcast_init(order_id, state_manager.get_final_vc(order_id, 1), order_data)
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

    return jsonify(status_data)


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
    app.run(host='0.0.0.0')
