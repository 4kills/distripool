import time
import zmq
from typing import Tuple


_orchestrator = None


class _Orchestrator:
    def __init__(self, listen_on: Tuple[str, str]):
        context = zmq.Context()

        # Socket to send messages to workers
        sender = context.socket(zmq.PUSH)
        sender.bind(f"tcp://{listen_on[0]}")

        # Socket to receive messages from workers
        receiver = context.socket(zmq.PULL)
        receiver.bind(f"tcp://{listen_on[1]}")

        # Send some work to be done
        for i in range(10):
            sender.send_string(str(i))

        # Collect the results
        results = []
        for i in range(10):
            result = receiver.recv_string()
            results.append(result)

        print(results)


def orchestrator() -> _Orchestrator:
    if _orchestrator is None:
        raise RuntimeError("make_orchestrator() has not been called yet.")
    return _orchestrator


def make_orchestrator(listen_on: Tuple[str, str] = ("*:1337", "*:1338")) -> _Orchestrator:
    orch = _Orchestrator(listen_on=listen_on)

    global _orchestrator
    if _orchestrator is None:
        _orchestrator = orch

    return orch

