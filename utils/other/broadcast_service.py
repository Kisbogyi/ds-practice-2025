import grpc.aio

import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2

HOSTNAMES = ["transaction_verification", "fraud_detection", "suggestions"]

async def broadcast(order_id: str, vector_clock: list[int]) -> None:
    # list of hostnames
    destinations: list[str] = HOSTNAMES
    for dst in destinations:
        async with grpc.aio.insecure_channel(f"{dst}:50054") as channel:
            # Create a stub object.
            stub = broadcast_grpc.BroadcastServiceStub(channel)
            # Call the service through the stub object.
            _ = await stub.BroadcastVC(
                broadcast_pb2.Message(order_id=order_id, vector_clock=vector_clock)
            )


async def broadcast_clear(order_id: str) -> None:
    destinations: list[str] = HOSTNAMES
    for dst in destinations:
        async with grpc.aio.insecure_channel(f"{dst}:50054") as channel:
            # Create a stub object.
            stub = broadcast_grpc.BroadcastClearStub(channel)

            _ = await stub.BroadcastClear(
                broadcast_pb2.ClearMessage(order_id=order_id)
            )

