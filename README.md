# sakaio

[![Build Status](https://img.shields.io/travis/nitely/sakaio.svg?style=flat-square)](https://travis-ci.org/nitely/sakaio)
[![Coverage Status](https://img.shields.io/coveralls/nitely/sakaio.svg?style=flat-square)](https://coveralls.io/r/nitely/sakaio)
[![pypi](https://img.shields.io/pypi/v/sakaio.svg?style=flat-square)](https://pypi.python.org/pypi/sakaio)
[![licence](https://img.shields.io/pypi/l/sakaio.svg?style=flat-square)](https://raw.githubusercontent.com/nitely/sakaio/master/LICENSE)

Swiss Army knife for asyncio. Mainly a playground
to work on my asyncio related ideas. Some of it production ready.

## Install

```
pip install sakaio
```

## Compatibility

Python +3.5

## Concurrent

Concurrent is the pythonic alternative
to asyncio's gather/wait. It'll always
wait for all tasks to finish and has proper
exception handling.

```python
import asyncio
import sakaio


async def factorial(name, number):
    f = 1
    for i in range(2, number + 1):
        print(f"Task {name}: Compute factorial({i})...")
        await asyncio.sleep(1)
        f *= i
    print(f"Task {name}: factorial({number}) = {f}")


async def main():
    # Schedule three calls *concurrently*:
    await sakaio.concurrent(
        factorial("A", 2),
        factorial("B", 3),
        factorial("C", 4))

asyncio.run(main())

# Expected output:
#
#     Task A: Compute factorial(2)...
#     Task B: Compute factorial(2)...
#     Task C: Compute factorial(2)...
#     Task A: factorial(2) = 2
#     Task B: Compute factorial(3)...
#     Task C: Compute factorial(3)...
#     Task B: factorial(3) = 6
#     Task C: Compute factorial(4)...
#     Task C: factorial(4) = 24
```

## Concurrent and Sequential pair

Concurrent + sequential provides a simple
way to describe tasks that have a complex
execution order

```python
import asyncio
import sakaio


async def sleepy(txt, sleep=0):
    await asyncio.sleep(sleep)
    print(txt)
    return txt


async def main():
    await sakaio.concurrent(
        sleepy("B", sleep=3),
        sleepy("A", sleep=2),
        sakaio.sequential(
            sleepy("C", sleep=4),
            sakaio.concurrent(
                sleepy("E", sleep=5),
                sleepy("D", sleep=2)),
            sleepy("F", sleep=1)))

asyncio.run(main())

# This prints:
#
# A
# B
# C
# D
# E
# F
```

## Tasks Guard

An unbounded tasks pool that waits for all tasks to finish
and provides proper exception propagation

```python
import asyncio
import sakaio


async def sleepy(txt, sleep=0):
    await asyncio.sleep(sleep)
    print(txt)


async def main():
    async with sakaio.TaskGuard() as pool:
        pool.create_task(sleepy("B", sleep=5))
        pool.create_task(sleepy("A", sleep=2))

asyncio.run(main())

# This prints
#
# A
# B
```

When one of the tasks raise an exception,
then all of the tasks gets cancelled
and the exception is raised

```python
import asyncio
import sakaio


async def sleepy(txt, sleep=0, raise_err=False):
    await asyncio.sleep(sleep)
    if raise_err:
        raise ValueError("Bad sleepy")
    print(txt)


async def main():
    async with sakaio.TaskGuard() as pool:
        pool.create_task(sleepy("B", sleep=5))
        pool.create_task(sleepy("A", sleep=2, raise_err=True))

asyncio.run(main())

# This prints a traceback (A raises and B gets cancelled)
```

## Wait

Similar to `asyncio.wait` except it cancels
all pending tasks and waits for them to finish

```python
import asyncio
import sakaio


async def sleepy(txt, sleep=0):
    await asyncio.sleep(sleep)
    print(txt)


async def main():
    await sakaio.wait([
        sleepy("B", sleep=5),
        sleepy("A", sleep=5)])

asyncio.run(main())

# This prints
#
# A
# B
```

## LICENSE

MIT
