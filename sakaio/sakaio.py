# -*- coding: utf-8 -*-

import asyncio


class RetList(list):
    pass


RETURN_EXCEPTIONS, CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE = range(3)


async def concurrent(
        *coros_or_futures,
        loop=None,
        exception_handling=CANCEL_TASKS_AND_RAISE):
    """
    A saner alternative to ``asyncio.gather``

    The ``exception_handling`` has three options \
    to control how exceptions are handled:

    ``RETURN_EXCEPTIONS`` will wait for \
    all tasks to finish and return everything as \
    values including exceptions

    ``WAIT_TASKS_AND_RAISE`` will wait for \
    all tasks to finish and raise the first \
    exception afterwards. \
    If a task is cancelled, it won't cancel the \
    remaining tasks and the resulting list will contain \
    the exception as value

    ``CANCEL_TASKS_AND_RAISE`` will raise the \
    first exception and cancel all remaining tasks. \
    If a task is cancelled, it won't cancel the \
    remaining tasks and the resulting list will contain \
    the exception as value

    It can be used in nested ``concurrent`` or \
    ``sequential`` calls and the resulting list will \
    get flattened

    In order to cancel a nested sequence of \
    ``concurrent/sequential``, the outer coroutine \
    must be created as a task.

    If this is created as a task and cancelled, then \
    all inner tasks will get cancelled as well.
    """
    loop = loop or asyncio.get_event_loop()

    if exception_handling == CANCEL_TASKS_AND_RAISE:
        return_when = asyncio.FIRST_EXCEPTION
    else:
        return_when = asyncio.ALL_COMPLETED

    futs = [
        asyncio.ensure_future(cf)
        for cf in coros_or_futures]

    try:
        done, pending = await asyncio.wait(
            futs,
            loop=loop,
            return_when=return_when)
    except asyncio.CancelledError:
        for fut in futs:
            fut.cancel()
        raise

    if pending:
        assert exception_handling == CANCEL_TASKS_AND_RAISE
        for fut in pending:
            fut.cancel()

    should_raise = exception_handling in (
        CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE)
    results = RetList()
    for fut in futs:
        if fut.cancelled():
            try:
                fut.result()
            except asyncio.CancelledError as err:
                results.append(err)
                continue

        if fut.exception() is not None:
            if should_raise:
                raise fut.exception()
            else:
                results.append(fut.exception())
            continue

        if isinstance(fut.result(), RetList):
            results.extend(fut.result())
        else:
            results.append(fut.result())
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
        coro_or_fut = asyncio.ensure_future(coro_or_fut, loop=loop)

        if cancel_all:
            coro_or_fut.cancel()
            continue

        try:
            ret = await asyncio.shield(coro_or_fut, loop=loop)
        except asyncio.CancelledError as err:
            if coro_or_fut.cancelled():
                if not return_exceptions:
                    error = err
                results.append(err)
            else:
                coro_or_fut.cancel()
                cancel_all = True
                error = err
            continue
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

        try:
            done, pending = await asyncio.wait(
                self._tasks,
                loop=self.loop,
                return_when=asyncio.FIRST_EXCEPTION)
        except asyncio.CancelledError:
            # TODO: test! (maybe it's not needed?)
            for t in self._tasks:
                t.cancel()
            raise
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
                try:
                    await asyncio.wait(pending, loop=self.loop)
                except asyncio.CancelledError:
                    # TODO: test! (maybe it's not needed?)
                    for t in pending:
                        t.cancel()
                    raise

    def create_task(self, coro):
        if self._state == self._closed:
            raise ValueError("Guard is closed")
        task = self.loop.create_task(coro)
        self._tasks.append(task)
        return task
