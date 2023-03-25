import zmq
from typing import Tuple
from multiprocessing import Pool as LocalPool


class _Worker:
    def __init__(self, connect_to: Tuple[str, str], processes: int | None = 1):
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(f"tcp://{connect_to[0]}")
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(f"tcp://{connect_to[1]}")
        self.processes = processes

    def start(self):
        while True:
            try:
                chunk_id, func, chunk = self.receiver.recv_pyobj()
            except zmq.error.ContextTerminated:
                return

            with LocalPool(processes=self.processes) as local_pool:
                result = local_pool.map(func, chunk)
            self.sender.send_pyobj((chunk_id, result))

    def close(self):
        self.receiver.close()
        self.sender.close()
        self.context.term()


def make_worker(orchestrator_address: Tuple[str, str], start: bool = True) -> _Worker:
    worker = _Worker(orchestrator_address)
    if start:
        worker.start()
    return worker

