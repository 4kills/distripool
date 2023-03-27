import os
import threading
from typing import Tuple, Callable, List, Any, Iterable, Literal
from distripool.orchestrator import _Orchestrator, default_orchestrator
from distripool.packet import DataPacket
from distripool.worker import make_worker
from distripool.asyncwrap import _AsyncResult


class Pool:
    def __init__(self, processes: int = None,
                 initializer=None,
                 initargs=(),
                 maxtasksperchild=None,
                 context=None,
                 orchestrator: _Orchestrator | None = None):
        self._processes = processes if processes is not None else os.cpu_count()
        self._initializer = initializer
        self._initargs = initargs
        self._maxtasksperchild=maxtasksperchild

        self.orchestrator = orchestrator if orchestrator is not None else default_orchestrator()

        self._terminated = False
        self._closed = False
        self._asyncs = []

        self.orchestrator._acquire()

        self._worker = make_worker(self.orchestrator.listen_on, start=False)
        threading.Thread(target=self._worker.start).start()

    def _map(self, func, iterable, chunksize, mapping_type: Literal['map', 'starmap'] = 'map'):
        if self._closed:
            raise ValueError("Pool not running")

        chunk_size = max(1, len(iterable) // self._processes) if chunksize is None else chunksize
        chunks = [iterable[i:i + chunk_size] for i in range(0, len(iterable), chunk_size)]
        for chunk_id, chunk in enumerate(chunks):
            self.orchestrator._send_work(DataPacket(chunk_id, func, chunk, mapping_type,
                                                    self._initializer, self._initargs, self._maxtasksperchild))

        unordered_results: List[Tuple[int, List[any, ...]]] = []
        for _ in chunks:
            result = self.orchestrator._receive_result()
            if result.holds_error():
                raise result.result
            unordered_results.append((result.id, result.result))

        sorted_results = sorted(unordered_results, key=lambda x: x[0])
        flattened_results = [result for chunked_results in sorted_results for result in chunked_results[1]]

        return flattened_results

    def _asynchronize(self, pool, target, args, callback, error_callback) -> _AsyncResult:
        async_result = _AsyncResult(pool, target, args, callback, error_callback)
        self._asyncs.append(async_result)
        return async_result

    def apply(self, func, args: Iterable = (), kwds={}):
        """
        Call func with arguments args and keyword arguments kwds. It blocks until the result is ready.
        Given this blocks, apply_async() is better suited for performing work in parallel.
        Additionally, func is only executed in one of the workers of the pool.
        """
        if not isinstance(args, Iterable):
            raise TypeError(f"args must be an Iterable, was {type(args)}")
        args = tuple(args)
        return self.starmap(func, [args + tuple(kwds.values())])[0]

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None) -> _AsyncResult:
        """
        A variant of the apply() method which returns an AsyncResult object.
        """
        return self._asynchronize(self, self.apply, (func, args, kwds), callback, error_callback)

    def map(self, func: Callable, iterable: List, chunksize: int = None) -> List:
        """
        A parallel equivalent of the map() built-in function (it supports only one iterable argument though).
        It blocks until the result is ready.

        This method chops the iterable into a number of chunks which it submits to the process pool as separate tasks.
        The (approximate) size of these chunks can be specified by setting chunksize to a positive integer.
        """
        return self._map(func, iterable, chunksize, 'map')

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None) -> _AsyncResult:
        """
        A variant of the map() method which returns a AsyncResult object.
        """
        return self._asynchronize(self, self.map, (func, iterable, chunksize), callback, error_callback)

    def imap(self, func, iterable, chunksize=1, parallel_calls=2):
        """
        A lazier version of map().

        The chunksize argument is the same as the one used by the map() method.
        For very long iterables using a large value for chunksize can make the job complete much faster than using the default value of 1.

        The parallel_calls argument indicates how many chunks should be produced at a time for worker nodes to process.
        """
        parallel_chunk_size = chunksize * parallel_calls
        parallel_chunks = [iterable[i:i + parallel_chunk_size] for i in range(0, len(iterable), parallel_chunk_size)]

        for parallel_chunk in parallel_chunks:
            chunk_result = self._map(func, parallel_chunk, chunksize=chunksize)
            for result in chunk_result:
                yield result

    def imap_unordered(self, func, iterable, chunksize=1, parallel_calls=2):
        """
        The same as imap() except that the ordering of the results from the returned iterator should be considered arbitrary.
        (Only when there is only one worker process is the order guaranteed to be “correct”.)

        Currently, this method merely calls self.imap(). However, expect this behavior to change in the future.
        """
        return self.imap(func, iterable, chunksize, parallel_calls)

    def starmap(self, func, iterable, chunksize=None):
        """
        Like map() except that the elements of the iterable are expected to be iterables that are unpacked as arguments.
        Hence an iterable of [(1,2), (3, 4)] results in [func(1,2), func(3,4)].
        """
        return self._map(func, iterable, chunksize, 'starmap')

    def starmap_async(self, func, iterable, chunksize=None, callback=None, error_callback=None):
        """
        A combination of starmap() and map_async() that iterates over iterable of iterables and calls func with the iterables unpacked. Returns a result object.
        """
        return self._asynchronize(self, self.starmap, (func, iterable, chunksize), callback, error_callback)

    def close(self):
        """
        Prevents any more tasks from being submitted to the pool. Once all the tasks have been completed the worker processes will exit.
        """
        self._closed = True

    def terminate(self):
        """
        Stops the worker processes immediately without completing outstanding work.
        When the pool object is garbage collected terminate() will be called immediately.
        """
        self._worker.close()
        self.orchestrator._release()
        self._terminated = True

    def join(self):
        """
        Wait for the worker processes to exit. One must call close() or terminate() before using join().
        """
        if not (self._closed or self._terminated):
            raise ValueError("Pool is still running")

        [a.wait() for a in self._asyncs]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

