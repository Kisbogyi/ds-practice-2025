import grpc
import logging
import order_executor.bullying_pb2 as bullying
import order_executor.bullying_pb2_grpc as bullying_grpc
import sys


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class HeartbeatService(bullying_grpc.HeartbeatServiceServicer):
    def Heartbeat(self, request, context):
        logger.info("Got heartbeat packet")
        return bullying.Pong()

def healthcheck(leander_ip) -> bool:
    """ Check the leader's health
    """
    try:
        with grpc.insecure_channel(f'{leander_ip}:50070') as channel:
            # Create a stub object.
            stub = bullying_grpc.HeartbeatServiceStub(channel)
            # Call the service through the stub object.
            k = stub.Heartbeat(bullying.Ping())
            logging.info(k)
            return True
    except grpc.RpcError:
        logger.info("Rpc error returned")
        return False

