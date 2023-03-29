from distripool import make_worker

# start after orchestrator_node

ip = '127.0.0.1'  # IP of orchestrator

if __name__ == '__main__':
    # blocks and executes work indefinitely.
    make_worker((f"{ip}:1337", f"{ip}:1338"))
