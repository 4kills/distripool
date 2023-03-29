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
        """
        Close the _Orchestrator instance, including its sockets and context.
        """
        self.sender.close()
        self.receiver.close()
        self.context.term()


def default_orchestrator() -> _Orchestrator:
    """
    :return: The default orchestrator created by the first call to 'make_orchestrator'.
    :except: RuntimeError if 'make_orchestrator' hasn't been called yet.
    """
    if _orchestrator is None:
        raise RuntimeError("make_orchestrator() has not been called yet.")
    return _orchestrator


def make_orchestrator(listen_on: Tuple[str, str] = ("*:1337", "*:1338")) -> _Orchestrator:
    """
    Create a new _Orchestrator instance and return it. If this is the first call to 'make_orchestrator',
    the created orchestrator will be set as default. If you do not plan to use multiple orchestrators or multiple pools in parallel,
    you do not have to worry about the processed orchestrator.

    :param listen_on: A tuple of two strings representing the addresses of the outbound and inbound sockets.
    The outbound socket is used to send data to the worker nodes for processing, while the inbound socket
    is used to receive results back from the workers.
    :return: An instance of _Worker.
    """
    orch = _Orchestrator(listen_on=listen_on)
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = orch

    return orch

