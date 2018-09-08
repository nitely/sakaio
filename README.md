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

## Concurrent and Sequential pair

Run tasks in a concurrent or sequential way of arbitrary nesting depth.

```python
import asyncio
import sakaio


async def sleepy(txt, sleep=0):
    await asyncio.sleep(sleep)
    print(txt)
    return txt


loop = asyncio.get_event_loop()
tasks = sakaio.concurrent(
    sleepy("B", sleep=3),
    sleepy("A", sleep=2),
    sakaio.sequential(
        sleepy("C", sleep=4),
        sakaio.concurrent(
            sleepy("E", sleep=5), sleepy("D", sleep=2)),
        sleepy("F", sleep=1)))
loop.run_until_complete(tasks)
loop.close()

# This prints:
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
    async with sakaio.TaskGuard(loop) as pool:
        pool.create_task(sleepy("B", sleep=5))
        pool.create_task(sleepy("A", sleep=2))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()

# This prints
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
    async with sakaio.TaskGuard(loop) as pool:
        pool.create_task(sleepy("B", sleep=5))
        pool.create_task(sleepy("A", sleep=2, raise_err=True))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()


# This prints a traceback (A raises and B gets cancelled)
```

## LICENSE

MIT
