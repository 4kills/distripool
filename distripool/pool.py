import threading
from typing import Tuple, Callable, List, Any
from distripool.orchestrator import _Orchestrator, default_orchestrator
from distripool.worker import make_worker
from distripool.asyncwrap import _AsyncResult


class Pool:
    def __init__(self, processes: int = None,
                 initializer=None,
                 initargs=(),
                 maxtasksperchild=None,
                 context=None,
                 orchestrator: _Orchestrator | None = None):
        self.processes = processes
        self.orchestrator = orchestrator if orchestrator is not None else default_orchestrator()
        self._terminated = False
        self.orchestrator._acquire()
        self._worker = make_worker(self.orchestrator.listen_on, start=False)
        threading.Thread(target=self._worker.start).start()

    def apply(self, func, args=(), kwds={}):
        """
        Call func with arguments args and keyword arguments kwds. It blocks until the result is ready.
        """
        raise NotImplementedError

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
        """
        A variant of the apply() method which returns an AsyncResult object.
        """
        raise NotImplementedError

    def map(self, func: Callable, iterable: List, chunksize: int = None) -> List:
        """
        A parallel equivalent of the map() built-in function (it supports only one iterable argument though).
        It blocks until the result is ready.

        This method chops the iterable into a number of chunks which it submits to the process pool as separate tasks.
        The (approximate) size of these chunks can be specified by setting chunksize to a positive integer.
        """
        chunk_size = max(1, len(iterable) // self.processes) if chunksize is None else chunksize
        chunks = [iterable[i:i + chunk_size] for i in range(0, len(iterable), chunk_size)]
        for chunk_id, chunk in enumerate(chunks):
            self.orchestrator._send_work((chunk_id, func, chunk))

        unordered_results: List[Tuple[int, List[any, ...]]] = []
        for _ in chunks:
            result = self.orchestrator._receive_result()
            if isinstance(result[1], Exception):
                raise result[1]
            unordered_results.append(result)

        sorted_results = sorted(unordered_results, key=lambda x: x[0])
        flattened_results = [result for chunked_results in sorted_results for result in chunked_results[1]]

        return flattened_results

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> _AsyncResult:
        """
        A variant of the map() method which returns a AsyncResult object.
        """
        return _AsyncResult(self, self.map, (func, iterable, chunksize), callback, error_callback)

    def imap(self, func, iterable, chunksize=1):
        """
        A lazier version of map().
        """
        raise NotImplementedError

    def imap_unordered(self, func, iterable, chunksize=1):
        """
        The same as imap() except that the ordering of the results from the returned iterator should be considered arbitrary.
        """
        raise NotImplementedError

    def starmap(self, func, iterable, chunksize=None):
        """
        Like map() except that the elements of the iterable are expected to be iterables that are unpacked as arguments.
        """
        raise NotImplementedError

    def starmap_async(self, func, iterable, chunksize=None, callback=None, error_callback=None):
        """
        A combination of starmap() and map_async() that iterates over iterable of iterables and calls func with the iterables unpacked. Returns a result object.
        """
        raise NotImplementedError

    def close(self):
        """
        Prevents any more tasks from being submitted to the pool. Once all the tasks have been completed the worker processes will exit.
        """
        self.orchestrator.close()

    def terminate(self):
        self._worker.close()
        self.orchestrator._release()
        self._terminated = True

    def join(self):
        """
        Wait for the worker processes to exit. One must call close() or terminate() before using join().
        """
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

