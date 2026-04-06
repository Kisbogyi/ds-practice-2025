import logging
import asyncio
import sys
import grpc.aio
from flask import Flask, request
from flask_cors import CORS
import json
import uuid
from typing import Any, Dict

import suggestions.suggestions_pb2 as suggestions
import suggestions.suggestions_pb2_grpc as suggestions_grpc

import fraud_detection.fraud_detection_pb2 as fraud_detection
import fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

import transaction_verification.transaction_verification_pb2 as transaction_verification
import transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

# TODO check if imports are correct
from utils.other.orderStateManager import OrderStateManager, VECTOR_CLOCK
from utils.other.orderResult import OrderResult
from utils.other.broadcast import broadcast_clear

logger = logging.getLogger(__name__)
state_manager = OrderStateManager(service_name="orchestrator")
order_results: Dict[str, OrderResult] = {}

# ================================= NEW COMM/GRPC (TO BE FIXED) ================================= # TODO replace

#
# It is possible to swich to just one broadcasts(event_type: str, order_id: str, incoming_vc: dict, payload: dict) for everethyng
# and have switch for event_type if it is annoying to do separate grpc calls for everething
#

async def transaction_init(order_id, trigger_vc, order_data) -> list[int]:
    username: str = order_data["username"]
    order_amount: int = order_data["username"]
    billing_address: str = order_data["username"]
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceInitStub(channel)
        # Call the service through the stub object.
        completionVC = await stub.CheckFraud(fraud_detection.FraudRequest(
            order_id=order_id,
            vc=trigger_vc,
            username=username,
            order_amount=order_amount,
            billing_address=billing_address,
        ))
    return completionVC.vc

async def broadcast_init(order_id: str, trigger_vc: dict, order_data: dict):
    logger.info(f"[BROADCAST INIT]: order {order_id}")
    # instead of broadcast do via grpc:
    # tr_trigger_vc = await transactions.init_order(order_id, trigger_vc, order_data)
    # fr_trigger_vc = await transaction_init(order_id, trigger_vc, order_data)
    # s_trigger_vc = await suggestions.init_order(order_id, state_manager._merge_clocks(tr_trigger_vc, fr_trigger_vc), order_data)
    pass  # TODO (ignore this, use merge for clear)
    # return state_manager._merge_clocks(trigger_vc, tr_trigger_vc, fr_trigger_vc, s_trigger_vc)


async def broadcast_clear(order_id: str):
    logger.info(f"[BROADCAST CLEAR]: order {order_id}")
    results = await asyncio.gather(
        # transactions.clear_order(order_id, trigger_vc, order_data),
        # fraud.clear_order(order_id, trigger_vc, order_data),
        # suggestions.clear_order(order_id, trigger_vc, order_data),
    )
    return all(results)  # TODO

# following are recived from microservices:

class FraudDetectionFinishHandler(fraud_detection_grpc.FraudDetectionServiceFinishedServicer):
    def FraudResponse(self, response, context):
        order_id = response.order_id
        success = response.success 
        reason = response.reason
        set_fraud_status(order_id, success, reason)

class TransactionVerificationServiceFinished(transaction_verification_grpc.TransactionVerificationServiceFinishedServicer):
    def VerificationResponse(self, response, context):
        order_id = response.order_id
        success = response.success 
        reason = response.reason
        set_transaction_status(order_id, success, reason)

def set_fraud_status(order_id: str, success: bool, reason: str = ""):
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
    result = OrderResult()
    order_id = str(uuid.uuid4())
    order_results[order_id] = result

    request_data = json.loads(request.data)

    # Print request object data
    logger.info(
        f"Checkout was called for order {order_id} with Request Data: {request_data.get('items')}")
    order_data = {
        "items": request_data.get('items', []),
        "user_name": request_data["user"]["name"],
        "card_number": request_data["creditCard"]["number"],
        "order_amount": len(request_data.get('items', [])),
        "billing_address": request_data["billingAddress"]
    }
    # note that it is already increased for trigger vc
    order = await state_manager.store_data(order_id, order_data)

    final_vc = await broadcast_init(order_id, {'orchestrator': 1}, order_data)  # INIT

    await state_manager.process_event(order_id)

    # wait and handle compleation
    try:
        # FIXME configure timeouts
        await asyncio.wait_for(result.wait(), timeout=10.0)
        if result.has_errors():
            logger.warning(f"Order {order_id}: {result.error}")
            status_data = {'orderId': order_id, "status": f"Order Rejected",
                           "suggestedBooks": [], "reason": str(result.error)}
        else:
            status_data = {'orderId': order_id, "status": "Order Accepted",
                        "suggestedBooks": result.suggestions}
    except asyncio.TimeoutError:
        logger.warning(f"Order {order_id}: Timed out.")
        status_data = {'orderId': order_id, "status": "Order Rejected",
                       "suggestedBooks": [], "reason": "Timed out"}

    # broadcast clear annd handle it
    try:
        if not await asyncio.wait_for(broadcast_clear(order_id, final_vc), timeout=10.0):
            status_data = {'orderId': order_id, "status": "Order Rejected",
                           "suggestedBooks": [], "reason": "Incorect Vector Clocks"}
    except asyncio.TimeoutError:
        logger.error(f"Order {order_id}: has time out in clear order broadcast")
        status_data = {'orderId': order_id, "status": "Order Rejected",
                           "suggestedBooks": [], "reason": "Timed out"}
    # clear
    order_results.pop(order_id, None)
    await state_manager.clear_data(order_id)

    response_payload = json.dumps(status_data)
    logger.info(f"Checkout response is: {response_payload}")
    return response_payload


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
