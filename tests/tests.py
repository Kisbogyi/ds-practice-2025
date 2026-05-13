import asyncio
import pytest
import requests
import time

# IMPORTANT! do not run this too many times in a row without restarting the compose
# because fraud detection will flag the same user placing too many orders at once

order_1 = {
        'user': {
            'name': 'John Doe',
            'contact': 'john.doe@example.com',
        },
        'creditCard': {
            'number': '4111111111111111',
            'expirationDate': '12/25',
            'cvv': '123',
        },
        'userComment': 'Please handle with care.',
        'items': [
            {
                'name': '1984',
                'quantity': 1,
            },
        ],
        'billingAddress': {
            'street': '123 Main St',
            'city': 'Springfield',
            'state': 'IL',
            'zip': '62701',
            'country': 'USA',
        },
        'shippingMethod': 'Standard',
        'giftWrapping': True,
        'termsAccepted': True,
    }

order_2 = {
    'user': {
        'name': 'Jane Doe',
        'contact': 'jane.doe@example.com',
    },
    'creditCard': {
        'number': '4111111111111111',
        'expirationDate': '12/26',
        'cvv': '124',
    },
    'userComment': 'Please handle with care.',
    'items': [
        {
            'name': 'Brave New World',
            'quantity': 3,
        },
        {
            'name': 'Fahrenheit 451',
            'quantity': 1,
        },
    ],
    'billingAddress': {
        'street': '124 Main St',
        'city': 'Springfield',
        'state': 'IL',
        'zip': '62701',
        'country': 'USA',
    },
    'shippingMethod': 'Standard',
    'giftWrapping': True,
    'termsAccepted': True,
}

# conflicts with order_1
order_3 = {
    'user': {
        'name': 'Jack Doe',
        'contact': 'jack.doe@example.com',
    },
    'creditCard': {
        'number': '4111111111111111',
        'expirationDate': '12/27',
        'cvv': '125',
    },
    'userComment': 'Please handle with care.',
    'items': [
        {
            'name': '1984',
            'quantity': 10,
        },
    ],
    'billingAddress': {
        'street': '125 Main St',
        'city': 'Springfield',
        'state': 'IL',
        'zip': '62701',
        'country': 'USA',
    },
    'shippingMethod': 'Standard',
    'giftWrapping': True,
    'termsAccepted': True,
}

# invalid cc number
order_4 = {
    'user': {
        'name': 'Jack Doe',
        'contact': 'jack.doe@example.com',
    },
    'creditCard': {
        'number': '5288203152618131',
        'expirationDate': '12/27',
        'cvv': '125',
    },
    'userComment': 'Please handle with care.',
    'items': [
        {
            'name': 'Dune',
            'quantity': 1,
        },
    ],
    'billingAddress': {
        'street': '125 Main St',
        'city': 'Springfield',
        'state': 'IL',
        'zip': '62701',
        'country': 'USA',
    },
    'shippingMethod': 'Standard',
    'giftWrapping': True,
    'termsAccepted': True,
}

def test_dummy():
    assert True

def test_single_valid_order():

    response = requests.post('http://localhost:8081/checkout', json=order_1)
    assert response.status_code == 200 and response.json()['status'] == "Order Approved"

@pytest.mark.asyncio
async def test_multiple_nonconflicting_orders():

    response_1 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_1)
    response_2 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_2)

    assert response_1.status_code == 200 and response_1.json()['status'] == "Order Approved"
    assert response_2.status_code == 200 and response_2.json()['status'] == "Order Approved"

@pytest.mark.asyncio
async def test_multiple_valid_invalid_orders():

    response_2 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_2)
    response_4 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_4)

    assert response_2.status_code == 200 and response_2.json()['status'] == "Order Approved"
    assert response_4.status_code == 200 and response_4.json()['status'] == "Order Rejected"

@pytest.mark.asyncio
async def test_multiple_conflicting_orders():

    response_1 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_1)
    response_3 = await asyncio.to_thread(requests.post, 'http://localhost:8081/checkout', json=order_3)

    assert response_1.status_code == 200
    assert response_3.status_code == 200

    assert response_1.json()['status'] == "Order Rejected" or response_3.json()['status'] == "Order Rejected"