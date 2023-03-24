import time
import zmq
from typing import Tuple


def make_worker():
    pass


class _Worker:
    def __init__(self, connect_to: Tuple[str, str]):
        context = zmq.Context()

        # Socket to receive messages from producer
        receiver = context.socket(zmq.PULL)
        receiver.connect(f"tcp://{connect_to[0]}")

        # Socket to send messages back to producer
        sender = context.socket(zmq.PUSH)
        sender.connect(f"tcp://{connect_to[1]}")

        # Process the work
        while True:
            work = receiver.recv_string()
            print("Processing work:", work)
            time.sleep(1)  # Simulate processing time
            result = "Result for " + work
            sender.send_string(result)
