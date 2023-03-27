import os

import zmq
from typing import Tuple
from multiprocessing import Pool as LocalPool

from distripool.packet import DataPacket, ResultPacket


class _Worker:
    def __init__(self, connect_to: Tuple[str, str], processes: int | None = None):
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(f"tcp://{connect_to[0]}")
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(f"tcp://{connect_to[1]}")

        self._processes = processes if processes is not None else os.cpu_count()
        self._initializer = None
        self._initargs = None
        self._maxtasksperchild = None

    def _set_pool_variables(self, data: DataPacket):
        self._initializer = data.initializer
        self._initargs = data.initargs
        self._maxtasksperchild = data.maxtasksperchild

    def _pool_vars_are_unequal(self, data: DataPacket) -> bool:
        return (data.initializer != self._initializer
                or data.initargs != self._initargs
                or data.maxtasksperchild != self._maxtasksperchild)

    def _execute_on_socket(self, func, *args):
        try:
            return func(*args), True
        except zmq.error.ContextTerminated:  # after using close() from another thread
            return None, False
        except zmq.error.ZMQError as e:
            if e.errno == 128:  # "not a socket" after using close() from another thread
                return None, False
            raise e

    def _send(self, payload: ResultPacket) -> bool:
        _, ok = self._execute_on_socket(self.sender.send_pyobj, payload)
        return ok

    def _receive(self) -> Tuple[DataPacket | None, bool]:
        return self._execute_on_socket(self.receiver.recv_pyobj)

    def _work_loop_with_initial(self, work: DataPacket) -> bool:
        with LocalPool(processes=self._processes, initializer=self._initializer, initargs=self._initargs,
                       maxtasksperchild=self._maxtasksperchild) as local_pool:
            while True:
                try:
                    result = work.choose_mapping(local_pool)(work.func, work.chunk)
                except Exception as e:
                    result = e

                ok = self._send(ResultPacket(work.id, result))
                if not ok:
                    return ok

                work, ok = self._receive()
                if not ok:
                    return ok

                if self._pool_vars_are_unequal(work):
                    self._set_pool_variables(work)
                    return True

    def start(self):
        """
        Start the _Worker instance to process tasks. Blocking.
        """
        work, ok = self._receive()
        if not ok:
            return

        self._set_pool_variables(work)

        while True:
            ok = self._work_loop_with_initial(work)
            if not ok:
                return

    def close(self):
        """
        Close the _Worker instance, including its sockets and context.
        This will terminate the work loop and thereby end any start() method, which will then return.
        """
        self.receiver.close()
        self.sender.close()
        self.context.term()


def make_worker(orchestrator_address: Tuple[str, str], start: bool = True) -> _Worker:
    """
    Create a new _Worker instance and call start on it if 'start' is set. Defaults to start the worker,
    which blocks the current thread.

    :param orchestrator_address: A tuple of two strings representing the addresses of the inbound and outbound sockets.
    The inbound socket is used to receive data to process from the orchestrator, while the outbound socket
    is used to send results back to the orchestrator.
    :param start: A boolean indicating whether to start the worker immediately. Defaults to True.
    :return: An instance of _Worker.
    """
    worker = _Worker(orchestrator_address)
    if start:
        worker.start()
    return worker

