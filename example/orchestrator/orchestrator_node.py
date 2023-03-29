from distripool import Pool, make_orchestrator


def square(x):
    return x * x

# start orchestrator_node before worker_node


if __name__ == '__main__':
    # before the first usage of Pool
    make_orchestrator()

    # some other code ...

    print("Wait for worker nodes to be started. Press enter to continue.")
    input()

    data = [i for i in range(10_000_000)]  # depending on your hardware, turn it down a notch ;)

    with Pool() as pool:
        results = pool.map(square, data)

    print(results)
