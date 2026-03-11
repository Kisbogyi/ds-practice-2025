import logging
import asyncio
import sys
import grpc.aio
from flask import Flask, request
from flask_cors import CORS
import json
import uuid

import suggestions.suggestions_pb2 as suggestions
import suggestions.suggestions_pb2_grpc as suggestions_grpc

import fraud_detection.fraud_detection_pb2 as fraud_detection
import fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

import transaction_verification.transaction_verification_pb2 as transaction_verification
import transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

logger = logging.getLogger(__name__)

# ================================= GRPC ================================= 
async def send_fraud_detection_grpc(
        username: str,
        order_amount: int,
        billing_address: str,
        order_id: str,
):
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        # Call the service through the stub object.
        response = await stub.CheckFraud(fraud_detection.FraudRequest(
            username=username,
            order_amount=order_amount,
            billing_address=billing_address,
        ))
    return response.is_fraud

async def check_fraud(username: str, order_amount: int, billing_address: str, order_id: str):
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling FraudRequest endpoint with:  username: {username}, order amount: {order_amount}, billing_address: {billing_address}")
    response = await send_fraud_detection_grpc(username, order_amount, billing_address, order_id)
    logger.info(f"FraudRequest responded with: {response}")
    return response


async def send_transaction_verification_grpc( card_number: str, order_amount: int, order_id: str):
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        # Call the service through the stub object.
        response = await stub.VerifyTransaction(transaction_verification.VerificationRequest(card_number=card_number, order_amount=order_amount))
    return response.is_valid

async def verify_transaction(card_number: str, order_amount: int, order_id: str):
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling TransactionVerification endpoint with:  card number: {card_number}, order amount: {order_amount}")
    response = await send_transaction_verification_grpc(card_number, order_amount, order_id)
    logger.info(f"TransactionVerification responded with: {response}")
    return response


async def get_suggestions_grpc(book_name: str, order_id: str) -> list[dict[str, str]]:
    async with grpc.aio.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        stub = suggestions_grpc.SuggestionsServiceStub(channel)
        # Call the service through the stub object.
        response = await stub.SuggestBook(suggestions.SuggestionRequest(book_name=book_name))
        recommended_books = zip(response.titles, response.authors, response.id)
        recommended_books = [{"bookId": book[2], "title": book[0], "author": book[1]} for book in recommended_books]
    return recommended_books 

async def suggest_books(book_name: str, order_id: str):
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling SuggestBook endpoint with:  book name: {book_name}")
    response = await get_suggestions_grpc(book_name, order_id)
    logger.info(f"SuggestBook responded with: {response}")
    return response

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
    # Get request object data to json
    request_data = json.loads(request.data)

    # create uuid
    order_id = str(uuid.uuid4())

    # Print request object data
    logger.info(f"Checkout was called for order {order_id} with Request Data: {request_data.get('items')}")
    credit_card_numer: str = request_data["creditCard"]["number"]
    order_amount: int = len(request_data["items"])
    billing_address: str = str(request_data["billingAddress"])
    logger.info(billing_address)
    username: str = request_data["user"]["name"]
    is_fraud, is_transaction_verified, book_suggestions = await asyncio.gather(
        check_fraud(username, order_amount, billing_address, order_id),
        verify_transaction(credit_card_numer, order_amount, order_id),
        suggest_books(request_data.get('items')[0]["name"], order_id)
    )

    order_approve_text = "Order Rejected" if is_fraud or not is_transaction_verified else "Order Approved"
    #TODO: order approved depend on fraud-detection stuff
    logger.info(book_suggestions)
    order_status_response = json.dumps({
        'orderId': order_id,
        'status': order_approve_text,
        'suggestedBooks': book_suggestions
    })
    logger.info(f"Checkout response is: {order_status_response}")
    return order_status_response


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
        
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
