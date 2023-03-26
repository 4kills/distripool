import zmq
from typing import Tuple, Callable, List, Any

from distripool.packet import DataPacket, ResultPacket

_orchestrator = None


class _Orchestrator:
    def __init__(self, listen_on: Tuple[str, str]):
        self.context = zmq.Context()
        self.listen_on = listen_on
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind(f"tcp://{listen_on[0]}")
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(f"tcp://{listen_on[1]}")
        self.free = True

    def _acquire(self):
        if not self.free:
            raise ValueError("The orchestrator was attempted to be acquired while it is not free. " +
                             "Perhaps it is in use by another Pool already. " +
                             "If you want to use two Pools simultaneously, create another orchestrator.")
        self.free = False

    def _release(self):
        self.free = True

    def _send_work(self, work: DataPacket):
        self.sender.send_pyobj(work)

    def _receive_result(self) -> ResultPacket:
        return self.receiver.recv_pyobj()

    def close(self):
        self.sender.close()
        self.receiver.close()
        self.context.term()


def default_orchestrator() -> _Orchestrator:
    if _orchestrator is None:
        raise RuntimeError("make_orchestrator() has not been called yet.")
    return _orchestrator


def make_orchestrator(listen_on: Tuple[str, str] = ("*:1337", "*:1338")) -> _Orchestrator:
    orch = _Orchestrator(listen_on=listen_on)
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = orch

    return orch

