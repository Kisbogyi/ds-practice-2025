import grpc
import logging
import sys

import utils.other.setup as setup
setup.initialize_pb_paths() # DO NOT TOUCH - IT DOESN'T WORK ON WIN WITHOUT!!!

import utils.pb.order_executor.bullying_pb2 as bullying
import utils.pb.order_executor.bullying_pb2_grpc as bullying_grpc


logger = setup.get_debug_logger(__name__)



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

