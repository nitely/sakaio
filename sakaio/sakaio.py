# -*- coding: utf-8 -*-

import asyncio


# XXX rename and make it public
async def _sane_wait(fs, *, loop=None, return_when=asyncio.ALL_COMPLETED):
    """Waits for all tasks to finish no matter what"""
    if not fs:
        return
    loop = loop or asyncio.get_event_loop()
    fs = [asyncio.ensure_future(fut, loop=loop) for fut in fs]
    try:
        done, pending = await asyncio.wait(
            fs, loop=loop, return_when=return_when)
    except asyncio.CancelledError:
        for fut in fs:
            fut.cancel()
        await asyncio.wait(fs, loop=loop)
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
    return _was_seen(ex.__context__, seen)


# XXX should skip one level of the stack,
#     so it won't show in traceback
def _chain_exceptions(excepts):
    """Chain a list of exceptions"""
    seen = set()
    ex = _unravel_exception(excepts[-1])
    _see_chain(excepts[-1], seen)
    for i in range(len(excepts) - 1, -1, -1):
        if _was_seen(excepts[i], seen):
            continue
        _see_chain(excepts[i], seen)
        ex.__context__ = excepts[i]
        ex = _unravel_exception(excepts[i])
    return excepts[-1]


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
    exception afterwards.

    ``CANCEL_TASKS_AND_RAISE`` will raise the \
    first exception and cancel all remaining tasks.

    It can be used in nested ``concurrent`` or \
    ``sequential`` calls and the resulting list will \
    get flattened

    In order to cancel a nested sequence of \
    ``concurrent/sequential``, the outer coroutine \
    must be created as a task.

    If this is created as a task and it's cancelled, then \
    all inner tasks will get cancelled as well.
    """
    assert exception_handling in (
        RETURN_EXCEPTIONS, CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE)
    if not coros_or_futures:
        return

    loop = loop or asyncio.get_event_loop()

    if exception_handling == CANCEL_TASKS_AND_RAISE:
        return_when = asyncio.FIRST_EXCEPTION
    else:
        return_when = asyncio.ALL_COMPLETED

    futs = [
        asyncio.ensure_future(cf, loop=loop)
        for cf in coros_or_futures]

    await _sane_wait(
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
            await _sane_wait([coro_or_fut], loop=loop)
            continue

        try:
            ret = await asyncio.shield(coro_or_fut, loop=loop)
        except asyncio.CancelledError as err:
            if coro_or_fut.cancelled():
                # Don't cancel sibling tasks
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

# XXX we should always wait for all tasks to finish,
#     even if they ignore cancellation, and chain
#     all exceptions afterwards.
class TaskGuard:
    """
    Create tasks and wait for them to finish.

    If a task raises an exception, then all remaining tasks
    get cancelled and the first exception is raised. Same thing if
    an exception gets raised within the ``with`` block

    If a task is cancelled and decides to ignore cancellation, the
    guard will still wait for it to finish
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
            await _sane_wait(self._tasks, loop=self.loop)

        try:
            await _sane_wait(
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
            await _sane_wait(self._tasks, loop=self.loop)

    def create_task(self, coro):
        if self._state == self._closed:
            raise ValueError("Guard is closed")
        task = self.loop.create_task(coro)
        self._tasks.append(task)
        return task
