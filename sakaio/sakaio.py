# -*- coding: utf-8 -*-

import asyncio


FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION
ALL_COMPLETED = asyncio.ALL_COMPLETED


async def wait(aws, *, loop=None, timeout=None, return_when=ALL_COMPLETED):
    """
    Waits for all tasks to finish no matter what.

    It return `None`. Futures or tasks must be passed \
    to retrieve the results later

    It'll cancel pending tasks and wait for them to finish \
    before returning

    Parameter `return_when` accept `sakaio.FIRST_COMPLETED`, \
    `sakaio.FIRST_EXCEPTION` and `sakaio.ALL_COMPLETED`
    """
    assert return_when in (
        FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED)
    if not aws:
        return
    loop = loop or asyncio.get_event_loop()
    futs = [asyncio.ensure_future(aw, loop=loop) for aw in aws]
    try:
        done, pending = await asyncio.wait(
            futs, loop=loop, timeout=timeout, return_when=return_when)
    except asyncio.CancelledError:
        for fut in futs:
            fut.cancel()
        await asyncio.wait(futs, loop=loop)
        raise
    else:
        if not pending:
            return
        for fut in pending:
            fut.cancel()
        try:
            await asyncio.wait(pending, loop=loop)
        except asyncio.CancelledError:
            await asyncio.wait(pending, loop=loop)
            raise


def _unravel_exception(ex):
    if ex.__context__ is None:
        return ex
    return _unravel_exception(ex.__context__)


def _see_chain(ex, seen):
    if ex is None:
        return
    seen.add(ex)
    _see_chain(ex.__context__, seen)
    # It seems cause == context,
    # but just to be safe,
    # follow that branch as well
    _see_chain(ex.__cause__, seen)


def _was_seen(ex, seen):
    if ex is None:
        return False
    if ex in seen:
        return True
    return (
        _was_seen(ex.__context__, seen) or
        _was_seen(ex.__cause__, seen))


def _chain_exceptions(excepts):
    """Chain a list of exceptions"""
    assert excepts
    # While chaining, we must not create cycles
    seen = set()
    _see_chain(excepts[0], seen)
    for i in range(len(excepts) - 1):
        if _was_seen(excepts[i + 1], seen):
            continue
        _see_chain(excepts[i + 1], seen)
        ex = _unravel_exception(excepts[i + 1])
        ex.__context__ = excepts[i]
    return excepts[-1]


class RetList(list):
    pass


RETURN_EXCEPTIONS, CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE = range(3)


async def concurrent(
        *aws,
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
    exception afterwards.

    ``CANCEL_TASKS_AND_RAISE`` will raise the \
    first exception, cancel all remaining tasks \
    and wait for them to finish

    It can be used in nested ``concurrent`` or \
    ``sequential`` calls and the resulting list will \
    get flattened.

    In order to cancel a nested sequence of \
    ``concurrent/sequential``, the outer coroutine \
    must be created as a task.

    If this is created as a task and it's cancelled, then \
    all inner tasks will get cancelled as well.

    If more than one task raise an exception, they get chained.
    """
    assert exception_handling in (
        RETURN_EXCEPTIONS, CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE)
    if not aws:
        return

    loop = loop or asyncio.get_event_loop()

    if exception_handling == CANCEL_TASKS_AND_RAISE:
        return_when = FIRST_EXCEPTION
    else:
        return_when = ALL_COMPLETED

    futs = [asyncio.ensure_future(aw, loop=loop) for aw in aws]

    await wait(
        futs,
        loop=loop,
        return_when=return_when)

    should_raise = exception_handling in (
        CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE)
    results = RetList()
    for fut in futs:
        if fut.cancelled():
            try:
                fut.result()
            except asyncio.CancelledError as err:
                if should_raise:
                    raise
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


# XXX exception_handling
async def sequential(*aws, loop=None, return_exceptions=False):
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
    for aw in aws:
        aw = asyncio.ensure_future(aw, loop=loop)

        if cancel_all:
            aw.cancel()
            await wait([aw], loop=loop)
            continue

        try:
            ret = await asyncio.shield(aw, loop=loop)
        except asyncio.CancelledError as err:
            if aw.cancelled():
                # Don't cancel sibling tasks
                if not return_exceptions:
                    error = err
                results.append(err)
            else:
                aw.cancel()
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
    an exception gets raised within the ``with`` block.

    If more than one task raise an exception, they get chained.

    If a task is cancelled and decides to ignore cancellation, the
    guard will still wait for it to finish.
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

        exs = []

        if exc_value is not None:
            exs.append(exc_value)
            for t in self._tasks:
                t.cancel()
            await wait(self._tasks, loop=self.loop)

        try:
            await wait(
                self._tasks,
                loop=self.loop,
                return_when=asyncio.FIRST_EXCEPTION)
        finally:
            for t in self._tasks:
                if t.cancelled():
                    continue
                if t.exception():
                    exs.append(t.exception())
            if exs:
                raise _chain_exceptions(exs)

    async def _clean_up(self):
        try:
            for t in self._tasks:
                if not t.done():
                    continue
                if t.cancelled():
                    continue
                if t.exception():
                    raise t.exception()
        finally:
            if not self._tasks:
                return
            for t in self._tasks:
                t.cancel()
            await wait(self._tasks, loop=self.loop)

    def create_task(self, coro):
        if self._state == self._closed:
            raise ValueError("Guard is closed")
        task = self.loop.create_task(coro)
        self._tasks.append(task)
        return task
