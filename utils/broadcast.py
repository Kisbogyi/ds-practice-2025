import grpc.aio
import pb.broadcast.broadcast_pb2 as broadcast
import pb.broadcast.broadcast_pb2_grpc as broadcast_grpc


async def broadcast(order_id: str, vector_clock: list[int]) -> None:
    # list of hostnames
    destinations: list[str] = [""]
    for dst in destinations:
        async with grpc.aio.insecure_channel(f"{dst}:50054") as channel:
            # Create a stub object.
            stub = broadcast_grpc.BroadcastServiceStub(channel)
            # Call the service through the stub object.
            _ = await stub.Broadcast(
                broadcast.Broadcast(order_id=order_id, vector_clock=vector_clock)
            )
