# -*- coding: utf-8 -*-

import asyncio


class RetList(list):
    pass


async def concurrent(*coros_or_futures, loop=None, return_exceptions=False):
    """
    Similar to ``asyncio.gather`` except it can be used in \
    nested ``concurrent`` or ``sequential`` calls and \
    the result list will get flattened
    """
    loop = loop or asyncio.get_event_loop()
    results = RetList()
    tasks = await asyncio.gather(
        *coros_or_futures, loop=loop, return_exceptions=return_exceptions)
    for ret in tasks:
        if isinstance(ret, RetList):
            results.extend(ret)
        else:
            results.append(ret)
    return results


async def sequential(*coros_or_futures, loop=None, return_exceptions=False):
    """
    Similar to ``concurrent`` except \
    the given tasks are ran sequentially

    If ``return_exceptions`` is ``False`` (the default), \
    the first exception will be raised and no coro \
    will run further. Otherwise, all coros will run and \
    exceptions will be appended to the returned list
    """
    loop = loop or asyncio.get_event_loop()
    results = RetList()
    cancel_all = False
    error = None
    for coro_or_fut in coros_or_futures:
        if cancel_all:
            asyncio.ensure_future(coro_or_fut, loop=loop).cancel()
            continue

        try:
            ret = await coro_or_fut
        except Exception as err:
            if not return_exceptions:
                cancel_all = True
                error = err
            results.append(err)
            continue

        if isinstance(ret, RetList):
            results.extend(ret)
        else:
            results.append(ret)

    if error:
        raise error
    return results


class TaskGuard:
    """
    Create tasks and wait for them to finish.

    If a task raises an exception, then all remaining tasks
    get cancelled and the first exception is raised. Same thing if
    an exception gets raised within the ``with`` block
    """

    _started, _running, _closed = range(3)

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self._tasks = []
        self._state = self._started

    async def __aenter__(self):
        if self._state != self._started:
            raise ValueError("Guard not started")
        self._state = self._running
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._state != self._running:
            raise ValueError("Guard not running")
        self._state = self._closed

        if not self._tasks:
            return

        # Cancel all if an exception was raised
        # within the context manager
        if exc_type is not None:
            for t in self._tasks:
                if t.done():
                    continue
                t.cancel()

        done, pending = await asyncio.wait(
            self._tasks,
            loop=self.loop,
            return_when=asyncio.FIRST_EXCEPTION)
        try:
            for t in done:
                if t.cancelled():
                    continue
                if t.exception():
                    raise t.exception()
        finally:
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.wait(pending, loop=self.loop)

    def create_task(self, coro):
        if self._state == self._closed:
            raise ValueError("Guard is closed")
        task = self.loop.create_task(coro)
        self._tasks.append(task)
        return task
