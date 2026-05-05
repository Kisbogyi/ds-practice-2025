import grpc.aio
import asyncio

import utils.pb.broadcast.broadcast_pb2_grpc as broadcast_grpc
import utils.pb.broadcast.broadcast_pb2 as broadcast_pb2

HOSTNAMES = ["transaction_verification", "fraud_detection", "suggestions", "order_queue"]

async def broadcast(order_id: str, vector_clock: list[int]) -> None:
    request = broadcast_pb2.Message(order_id=order_id,vector_clock=vector_clock)
    await asyncio.gather(*[_send(dst, "vc", request)for dst in HOSTNAMES])


async def broadcast_clear(order_id: str) -> None:
    request = broadcast_pb2.ClearMessage(order_id=order_id)
    await asyncio.gather(*[_send(dst, "clear", request)for dst in HOSTNAMES])

async def _send(dst: str, method: str, request):
    try:
        async with grpc.aio.insecure_channel(f"{dst}:50054") as channel:
            if method == "vc":
                stub = broadcast_grpc.BroadcastServiceStub(channel)
                await stub.BroadcastVC(request)
            elif method == "clear":
                stub = broadcast_grpc.BroadcastClearStub(channel)
                await stub.BroadcastClear(request)
    except Exception as e:
        print(f"[Broadcast ERROR] {dst}: {e}")