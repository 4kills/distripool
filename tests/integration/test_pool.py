import threading
import unittest
from distripool.orchestrator import make_orchestrator
from distripool.pool import Pool
from distripool.worker import make_worker


def square(x):
    return x * x


class TestDistributedPool(unittest.TestCase):
    workers = []

    def setUp(self):
        self.nb_workers = 1

        self.orchestrator = make_orchestrator(("127.0.0.1:1337", "127.0.0.1:1338"))

        for _ in range(self.nb_workers):
            worker = make_worker(("127.0.0.1:1337", "127.0.0.1:1338"), start=False)
            threading.Thread(target=worker.start).start()
            self.workers.append(worker)

    def tearDown(self):
        [w.close() for w in self.workers]
        self.orchestrator.close()

    def test_map(self):
        data = [i for i in range(20)]

        with Pool(processes=4) as pool:
            result = pool.map(square, data)

        expected_result = [square(x) for x in data]
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
