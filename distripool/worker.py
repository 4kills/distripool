import zmq
from typing import Tuple
from multiprocessing import Pool as LocalPool

from distripool.packet import DataPacket, ResultPacket


class _Worker:
    def __init__(self, connect_to: Tuple[str, str], processes: int | None = 1):
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(f"tcp://{connect_to[0]}")
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(f"tcp://{connect_to[1]}")

        self._processes = processes
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
        work, ok = self._receive()
        if not ok:
            return

        self._set_pool_variables(work)

        while True:
            ok = self._work_loop_with_initial(work)
            if not ok:
                return

    def close(self):
        self.receiver.close()
        self.sender.close()
        self.context.term()


def make_worker(orchestrator_address: Tuple[str, str], start: bool = True) -> _Worker:
    worker = _Worker(orchestrator_address)
    if start:
        worker.start()
    return worker

