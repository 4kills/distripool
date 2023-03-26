import multiprocessing
import concurrent.futures as ft


class _FutureAsync:
    @staticmethod
    def _nop(x):
        pass

    def __init__(self, target, args, callback, error_callback):
        self._result = None

        if callback is None:
            callback = _FutureAsync._nop
        if error_callback is None:
            error_callback = _FutureAsync._nop

        executor = ft.ThreadPoolExecutor(max_workers=1)
        self.future = executor.submit(target, *args)

        def handler(f: ft.Future):
            e = f.exception()
            if e is None:
                callback(f.result())
                return
            error_callback(e)

        self.future.add_done_callback(handler)
        executor.shutdown(cancel_futures=False, wait=False)

    def ready(self):
        return self.future.done()

    def successful(self):
        return self.future.exception() is None

    def wait(self, timeout=None):
        try:
            self._result = self.future.result(timeout=timeout)
        except ft._base.TimeoutError as e:
            raise multiprocessing.TimeoutError(e)

    def get(self, timeout=None) -> any:
        self.wait(timeout)
        return self._result


class _AsyncResult:
    def __init__(self, pool, target, args, callback, error_callback):
        self._pool = pool
        self._async = _FutureAsync(target, args, callback, error_callback)

    def _check_terminated(self):
        if self._pool._terminated:
            raise ValueError("Pool that returned this AsyncResult was terminated before the computation completed. " +
                             "Call Pool.terminate after being done with this AsyncResults. " +
                             "Beware the use of Pool as Context.")

    def ready(self):
        self._check_terminated()
        return self._async.ready()

    def successful(self):
        if not self.ready():
            raise ValueError("Call to 'successful' before AsyncResult is ready as per 'ready'.")
        return self._async.successful()

    def wait(self, timeout=None):
        self._check_terminated()
        self._async.wait(timeout)

    def get(self, timeout=None) -> any:
        self._check_terminated()
        return self._async.get(timeout)
