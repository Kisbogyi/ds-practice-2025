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
order_results: Dict[str, OrderResult] = {} # TODO locking

# ================================= NEW COMM/GRPC (TO BE FIXED) ================================= # TODO replace

#
# It is possible to swich to just one broadcasts(event_type: str, order_id: str, incoming_vc: list[int], payload: dict) for everethyng
# and have switch for event_type if it is annoying to do separate grpc calls for everething
#


async def broadcast_init(order_id: str, trigger_vc: dict, order_data: dict):
    logger.info(f"[BROADCAST INIT]: order {order_id}")
    # instead of broadcast do via grpc:
    # [tr_trigger_vc, fr_trigger_vc] = asyncio.gather(
    #   await transactions.init_order(order_id, trigger_vc, order_data)
    #   await fraud.init_order(order_id, trigger_vc, order_data)
    # )
    # s_trigger_vc = await suggestions.init_order(order_id, state_manager.merge_clocks(tr_trigger_vc, fr_trigger_vc), order_data)
    pass  # TODO (ignore this, use merge for clear)
    # return s_trigger_vc


async def broadcast_clear(order_id: str):
    logger.info(f"[BROADCAST CLEAR]: order {order_id}")
    results = await asyncio.gather(
        # transactions.clear_order(order_id, trigger_vc, order_data),
        # fraud.clear_order(order_id, trigger_vc, order_data),
        # suggestions.clear_order(order_id, trigger_vc, order_data),
    )
    return all(results)  # TODO

# following are recived from microservices:


def set_fraud_status(order_id: str, success: bool, reason: str = None):
    if success:
        order_results[order_id].pass_verefication()
    else:
        order_results[order_id].fail(Exception(reason))


def set_transaction_status(order_id: str, success: bool, reason: str):
    if success:
        order_results[order_id].pass_transaction()
    else:
        order_results[order_id].fail(Exception(reason))


def set_suggestions(order_id: str, suggestions: Dict):
    order_results[order_id].set_suggestions(suggestions)

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
