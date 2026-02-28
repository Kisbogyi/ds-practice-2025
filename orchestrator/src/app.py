import logging
import sys
import os
import grpc
# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS
import json

import suggestions.suggestions_pb2 as suggestions
import suggestions.suggestions_pb2_grpc as suggestions_grpc
# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
import fraud_detection.fraud_detection_pb2 as fraud_detection
import fraud_detection.fraud_detection_pb2_grpc as fraud_detection_grpc

# FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
# transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
# sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification.transaction_verification_pb2 as transaction_verification
import transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc

# FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
# suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
# sys.path.insert(0, suggestions_grpc_path)

logger = logging.getLogger(__name__)

# ================================= GRPC ================================= 
def send_fraud_detection_grpc(card_number: str, order_amount: str) -> bool:
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        # Create a stub object.
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        # Call the service through the stub object.
        response = stub.CheckFraud(fraud_detection.FraudRequest(card_number=card_number, order_amount=order_amount))
    return response.is_fraud

def check_fraud(card_number: str, order_amount: str) -> bool:
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling FraudRequest endpoint with:  card number: {card_number}, order amount: {order_amount}")
    response = send_fraud_detection_grpc(card_number, order_amount)
    logger.info(f"FraudRequest responded with: {response}")
    return response


def send_transaction_verification_grpc( card_number: str, order_amount: str) -> bool:
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        # Call the service through the stub object.
        response = stub.VerifyTransaction(transaction_verification.VerificationRequest(card_number=card_number, order_amount=order_amount))
    return response.is_valid

def verify_transaction(card_number: str, order_amount: str) -> bool:
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling TransactionVerification endpoint with:  card number: {card_number}, order amount: {order_amount}")
    response = send_transaction_verification_grpc(card_number, order_amount)
    logger.info(f"TransactionVerification responded with: {response}")
    return response


def get_suggestions_grpc(book_name: str, book_style: str) -> list[str]:
    with grpc.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        stub = suggestions_grpc.SuggestionsServiceStub(channel)
        # Call the service through the stub object.
        response = stub.SuggestBook(suggestions.SuggestionRequest(book_name=book_name, book_style=book_style))
    return response.recommendations

def suggest_books(book_name: str, book_genre: str) -> list[str]:
    # Establish a connection with the fraud-detection gRPC service.
    logger.info(f"Calling SuggestBook endpoint with:  book name: {book_name}, book genre: {book_genre}")
    response = get_suggestions_grpc(book_name, book_genre)
    logger.info(f"SuggestBook responded with: {response}")
    return response

# ================================= WEBSERVER ================================= 

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # Get request object data to json
    request_data = json.loads(request.data)
    # Print request object data
    logger.info(f"Checkout was called with Request Data: {request_data.get('items')}")
    credit_card_numer: str = request_data["creditCard"]["number"]
    order_amount: str = str(len(request_data["items"]))
    is_fraud = check_fraud(credit_card_numer, order_amount)
    is_valid_transaction = verify_transaction(credit_card_numer, order_amount)
    books = suggest_books("The Foundation", "sci-fi")

    order_approve_text = "Order Approved" if is_fraud else "Order Rejected"
    # Dummy response following the provided YAML specification for the bookstore
    #TODO: order approved depend on fraud-detection stuff
    order_status_response = {
        'orderId': '12345',
        'status': order_approve_text,
        'suggestedBooks': [
            {'bookId': '123', 'title': 'The Best Book', 'author': 'Author 1'},
            {'bookId': '456', 'title': 'The Second Best Book', 'author': 'Author 2'}
        ]
    }
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
