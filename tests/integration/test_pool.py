import multiprocessing
import threading
import time
import unittest
from distripool.orchestrator import make_orchestrator
from distripool.pool import Pool
from distripool.worker import make_worker


def square(x):
    return x * x


def sum(a, b):
    return a + b


def raise_error(x):
    raise ValueError("some_error")


def wait(x):
    time.sleep(x)
    return x


class TestDistributedPool(unittest.TestCase):
    workers = []
    data = [i for i in range(20)]
    tuples_data = [(i, i+1) for i in range(20)]
    expected_squares = [square(x) for x in data]
    expected_sums = [sum(*e) for e in tuples_data]
    random_time = [0.148, 0.187, 0.206, 0.281, 0.219, 0.209, 0.120, 0.226, 0.290, 0.179, 0.167, 0.263, 0.259, 0.238, 0.162, 0.274, 0.156, 0.296, 0.277, 0.291]

    def setUp(self):
        self.nb_workers = 1

        self.orchestrator = make_orchestrator(("127.0.0.1:1337", "127.0.0.1:1338"))
        self.base_pool = Pool(processes=4, orchestrator=self.orchestrator)

        for _ in range(self.nb_workers):
            worker = make_worker(("127.0.0.1:1337", "127.0.0.1:1338"), start=False)
            threading.Thread(target=worker.start).start()
            self.workers.append(worker)

    def tearDown(self):
        self.base_pool.terminate()
        [w.close() for w in self.workers]
        self.orchestrator.close()

    def test_map(self):
        result = self.base_pool.map(square, self.data)
        self.assertEqual(result, self.expected_squares)

    def test_starmap(self):
        result = self.base_pool.starmap(sum, self.tuples_data)
        self.assertEqual(result, self.expected_sums)

    def test_map_async(self):
        result_async = self.base_pool.map_async(square, self.data)
        self.assertEqual(result_async.get(), self.expected_squares)

    def test_map_async_wait(self):
        result_async = self.base_pool.map_async(square, self.data)
        result_async.wait()
        self.assertEqual(result_async.get(), self.expected_squares)

    def test_map_async_raise_error(self):
        result_async = self.base_pool.map_async(raise_error, self.data)
        with self.assertRaises(ValueError):
            result_async.get()

    def test_map_async_timeout(self):
        result_async = self.base_pool.map_async(time.sleep, self.random_time)
        with self.assertRaises(multiprocessing.TimeoutError):
            result_async.get(timeout=2)

    def test_map_async_callback(self):
        semaphore = threading.Semaphore(1)
        semaphore.acquire()

        def callback(x):
            self.assertEqual(x, self.expected_squares)
            semaphore.release()

        self.base_pool.map_async(square, self.data, callback=callback)
        semaphore.acquire()

    def test_map_async_error_callback(self):
        semaphore = threading.Semaphore()
        semaphore.acquire()

        def error_callback(x):
            self.assertTrue(isinstance(x, ValueError))
            semaphore.release()

        self.base_pool.map_async(raise_error, self.data, error_callback=error_callback)
        semaphore.acquire()

    def test_starmap_async(self):
        result = self.base_pool.starmap_async(sum, self.tuples_data)
        self.assertEqual(result.get(), self.expected_sums)

    def test_apply(self):
        with self.assertRaises(TypeError):
            self.base_pool.apply(square, 3)

        res = self.base_pool.apply(square, (3,))
        self.assertEqual(res, 9)

        res = self.base_pool.apply(sum, (3, 4))
        self.assertEqual(res, 7)

    def test_apply_async(self):
        res = self.base_pool.apply_async(sum, (3, 4))
        self.assertEqual(res.get(), 7)


if __name__ == "__main__":
    unittest.main()
