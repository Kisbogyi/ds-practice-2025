import logging
import grpc
import socket
import sys
import order_executor.bullying_pb2 as bullying
import order_executor.bullying_pb2_grpc as bullying_grpc

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('<%(levelname)s> %(asctime)s %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class CoordinatorService(bullying_grpc.CoordinatorServicer):
    def __init__(self, executor_service):
        self.executor_service = executor_service

    def Heartbeat(self, request, context):
        new_leader = context.peer().split(':')[1] 
        logger.info(f"New leader is: {new_leader}!")
        self.executor_service.leader_ip = new_leader
        return bullying.Pong()

class ElectionService(bullying_grpc.ElectionServicer):
    def __init__(self, executor_service):
        self.executor_service = executor_service

    def Heartbeat(self, request, context):
        logger.info("Got election packet")
        logger.info("Started bulltying")
        bully(self.executor_service)

        logger.info("Responding to Election")
        return bullying.Pong()


def get_container_ip() -> str:
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return ip_address

def get_ips(hostname: str) -> list[str]:
    """Resolves hostname
    This resolves the hostname which can be used to get all the replicas' ips

    args:
        hostname: which hostname to resolve with DNS

    returns:
        list[str]: ip addresses which the host resolves to
    """
    sockets = socket.getaddrinfo(hostname, 50070, proto=socket.IPPROTO_TCP)
    return [str(sock[4][0]) for sock in sockets]

def election(ip: str):
    try:
        with grpc.insecure_channel(f'{ip}:50070') as channel:
            # Create a stub object.
            logger.info(f"sending election packet to: {ip}")
            stub = bullying_grpc.ElectionStub(channel)
            # Call the service through the stub object.
            stub.Heartbeat(bullying.Ping())
        return True
    except grpc.RpcError:
        logger.error(f"Could not send election packet to {ip}!")
        return False

def coordination(ip: str) -> None:
    try:
        with grpc.insecure_channel(f'{ip}:50070') as channel:
            # Create a stub object.
            stub = bullying_grpc.CoordinatorStub(channel)
            # Call the service through the stub object.
            stub.Heartbeat(bullying.Ping())
    except grpc.RpcError:
        logger.error(f"Could not send coordination packet to {ip}!")

            

def bully(executor_service):
    logger.info("started bullying")
    my_ip = get_container_ip()
    logger.info(f"My current ip is: {my_ip}")
    ips = get_ips("order_executor")
    logger.info(f"The replica ips are the following: {ips}")
    highest = True
    for ip in ips:
        # TODO: this does not work
        if ip > my_ip:
            if election(ip):
                logger.info(f"Host: {ip} responded!")
                highest = False
                break

    if highest:
        logger.info("Non of the hosts responded!") 
        executor_service.leader_ip = my_ip
        logger.info("New leader ip: {my_ip}")
        for ip in ips:
            coordination(ip)
