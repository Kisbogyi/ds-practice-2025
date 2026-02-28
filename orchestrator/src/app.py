import asyncio 
import sys
import os
# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS
import json


# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc


import grpc

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
    return send_fraud_detection_grpc(card_number, order_amount)


def send_transaction_verification_grpc( card_number: str, order_amount: str) -> bool:
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        # Create a stub object.
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        # Call the service through the stub object.
        response = stub.VerifyTransaction(transaction_verification.VerificationRequest(card_number=card_number, order_amount=order_amount))
    return response.is_valid

def verify_transaction(card_number: str, order_amount: str) -> bool:
    # Establish a connection with the fraud-detection gRPC service.
    return send_transaction_verification_grpc(card_number, order_amount)


def get_suggestions_grpc(book_name: str, book_style: str) -> list[str]:
    with grpc.insecure_channel('suggestions:50053') as channel:
        # Create a stub object.
        stub = suggestions_grpc.SuggestionsServiceStub(channel)
        # Call the service through the stub object.
        response = stub.SuggestBook(suggestions.SuggestionRequest(book_name=book_name, book_style=book_style))
    return response.recommendations

def suggest_books(book_name: str, book_style: str) -> list[str]:
    # Establish a connection with the fraud-detection gRPC service.
    return get_suggestions_grpc(book_name, book_style)

# ================================= WEBSERVER ================================= 

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.
@app.route('/', methods=['GET'])
def index():
    """
    Responds with 'Hello, [name]' when a GET request is made to '/' endpoint.
    """
    # Test the fraud-detection gRPC service.
    response = "dummy"
    # Return the response.
    return response

@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # Get request object data to json
    request_data = json.loads(request.data)
    # Print request object data
    print("Request Data:", request_data.get('items'))
    credit_card_numer: str = request_data["creditCard"]["number"]
    order_amount: str = str(len(request_data["items"]))
    is_fraud = check_fraud(credit_card_numer, order_amount)
    is_valid_transaction = verify_transaction(credit_card_numer, order_amount)
    books = suggest_books("The Foundation", "sci-fi")
    print(books)

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

    return order_status_response


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
