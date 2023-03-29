# Distripool
Distripool is a Python library that provides a simple way to distribute tasks across multiple worker
processes in a parallel and asynchronous manner. 
The library offers an easy-to-use API implementing the interface of the multiprocessing.Pool of the standard library, 
with additional support for distributed computing using multiple clusters of worker nodes. 
Distripool may be used as drop-in replacement of the multiprocessing.Pool. 

See `example` for a ready-to-use setup across multiple nodes.  

```diff
- This library is a potential security hazard! See limitations for details.
```

## Features

- Distributed computing with multiple clusters of worker nodes
- Asynchronous task execution with timeouts and callbacks
- Parallel map, apply, and starmap implementations
- Lazy iterator-based versions of map and apply (imap, imap_unordered)
- Customizable orchestrator for managing worker processes

## Installation
 
As of now, this module is not available via `pip`. 
However, support will be added once this project completes alpha stage.

## Usage

Before the first usage of `Pool` you will have to call `make_orchestrator()` once in the code you want to 
use `Pool` in and execute a simple Python script calling `make_worker()` on each worker node you want to use. 
Be aware that `make_orchestartor()` has to be called before `make_worker()`.

See `example` for a ready-to-use setup across multiple nodes.  

### Basic Usage: 

Here's a simple example of using Distripool to parallelize the execution of a function across multiple worker nodes:

On the worker device use: 
```python
from distripool import make_worker

ip = '127.0.0.1' # IP of orchestrator

if __name__ == '__main__':
    # blocks and executes work indefinitely.
    make_worker((f"{ip}:1337", f"{ip}:1338")) 
```

On the orchestrator, where you want to use the Pool:

```python
from distripool import Pool, make_orchestrator


def square(x):
    return x * x


if __name__ == '__main__':
    # somewhere before the first usage of Pool:
    make_orchestrator()

    # some other code ... 

    data = [i for i in range(20)]
    
    with Pool() as pool:
        results = pool.map(square, data)
    
    print(results)
```

### Advanced usage:

If you want to use imports in your function to be distributed, import them in the function itself 
and make sure the libraries are available on the worker nodes:

```python
def calculate_twos_logarithm(x):
    import math
    
    return math.log(x, 2)

with Pool() as pool:
    results = pool.map(calculate_twos_logarithm, [1, 2, 3])
```

You can also use the asynchronous version of map:

```python
# same square, data as above

with Pool() as pool:
    # does not block
    async_results = pool.map_async(square, data)
    
    # execute other code here ... 
    
    # get blocks until results are ready
    results = async_results.get()

print(results)
```

Distripool also supports starmap, which takes an iterable of argument tuples:

```python
def add(x, y):
    return x + y

data = [(i, i + 1) for i in range(20)]

with Pool() as pool:
    results = pool.starmap(add, data)

print(results)
```

To create and manage multiple orchestrators, in order to use multiple Pools _in parallel_, use make_orchestrator:

```python
from distripool import Pool, make_orchestrator

orchestrator = make_orchestrator(("127.0.0.1:1337", "127.0.0.1:1338"))

with Pool(orchestrator=orchestrator) as pool:
    # perform tasks with your custom orchestrator
    pass

# close it once you're done with it in order to free the ports defined above.
orchestrator.close()
```

## Limitations

### Security Limitations:

This library uses `pickle` and distributes the function to be executed on the worker nodes as pickled objects. 
Therefore, an attacker can send their own function to the worker nodes and, thereby, **execute arbitrary code** on your workers!

Please proceed with caution when using this library.  
If you do _not_ understand the implications and have no means to protect your worker nodes, do not use this library.

### Feature Limitations:

- Function Closures, i.e. including variables of the outer scope(s) in your function to be distributed, is not supported
- As for `multiprocessing.Pool`, the function to be distributed must be a top-level function. 
- Any imports needed in the function to be distributed must be used directly in the function and available on the worker nodes.
- `distripool.Pool` has, in general, the same limitations as `multiprocessing.Pool` in regard to the function to be distributed. 

## Documentation
For more detailed documentation on the available functions and classes, please refer to the source code docstrings.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue to discuss your changes or report bugs.

## License
Distripool is released under the GPL-3 License. See the LICENSE file for details.
