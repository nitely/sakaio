# -*- coding: utf-8 -*-

import asyncio
import logging

logger = logging.getLogger('sakaio')


def _all_tasks(loop=None):
    """For compat with py3.5 and py3.6"""
    try:
        return asyncio.all_tasks(loop=loop)
    except AttributeError:
        return {t for t in asyncio.Task.all_tasks(loop=loop) if not t.done()}


async def cancel_all_tasks(*, timeout=None, raise_timeout_error=False):
    """
    Cancel all tasks and wait for cancellation.\
    Use this before terminating the program to allow\
    tasks clean-up.

    Task errors are logged as warnings. Tasks that did not finish\
    are logged as warnings. The logger is called ``sakaio``.

    Raise ``asyncio.TimeoutError`` if ``raise_timeout_error=True`` and\
    the provided timeout is reached.\
    This function may never return if a timeout is not provided, since\
    tasks are allowed to ignore cancellation.

    asyncio's `run()` does something similar at exit, except it has no timeout.

    Usage::

        try:
            loop.run_until_complete(coro)
        finally:
            loop.run_until_complete(sakaio.cancel_all_tasks(timeout=10))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    """
    def _warn_pending():
        running = _all_tasks(loop=loop)
        if running:
            logger.warning(
                'There are %s pending tasks, first 10: %r',
                len(running), list(running)[:10])

    loop = asyncio.get_event_loop()
    running = _all_tasks(loop=loop)
    for t in running:
        t.cancel()
    for f in asyncio.as_completed(running, timeout=timeout, loop=loop):
        try:
            await f
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            _warn_pending()
            if raise_timeout_error:
                raise
            return
        except Exception:
            logger.warning('Task Error!', exc_info=True)
            pass
    # Tasks scheduled by clean-ups or
    # by tasks ignoring cancellation
    _warn_pending()


FIRST_COMPLETED = asyncio.FIRST_COMPLETED
FIRST_EXCEPTION = asyncio.FIRST_EXCEPTION
ALL_COMPLETED = asyncio.ALL_COMPLETED


# XXX remove
async def wait(aws, *, loop=None, timeout=None, return_when=ALL_COMPLETED):
    """
    Waits for all tasks to finish no matter what.

    Return a list of futures ordered by ``aws``.

    It'll cancel pending tasks and wait for them to finish \
    before returning

    Parameter ``return_when`` accept ``sakaio.FIRST_COMPLETED``, \
    ``sakaio.FIRST_EXCEPTION`` and ``sakaio.ALL_COMPLETED``
    """
    assert return_when in (
        FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED)
    if not aws:
        return []
    loop = loop or asyncio.get_event_loop()
    fs = [asyncio.ensure_future(aw, loop=loop) for aw in aws]
    try:
        done, pending = await asyncio.wait(
            fs, loop=loop, timeout=timeout, return_when=return_when)
    except asyncio.CancelledError:
        for f in fs:
            f.cancel()
        await asyncio.wait(fs, loop=loop)
        raise

    if not pending:
        return fs
    for f in pending:
        f.cancel()
    try:
        await asyncio.wait(pending, loop=loop)
    except asyncio.CancelledError:
        await asyncio.wait(pending, loop=loop)
        raise

    return fs


async def _wait_completion(fs, *, loop, cancel_on_first_ex=False):
    assert fs
    fs = set(fs)
    waiter = loop.create_future()
    counter = len(fs)
    result = []
    exiting = False

    def _on_completion(f):
        nonlocal counter, result, exiting
        assert counter > 0
        result.append(f)
        counter -= 1
        if counter <= 0:
            if not waiter.done():
                assert len(result) == len(fs)
                waiter.set_result(result)
        if (not exiting and
                cancel_on_first_ex and
                not f.cancelled() and
                f.exception()):
            exiting = True
            for ff in fs:
                ff.cancel()

    for f in fs:
        # XXX test if already done
        f.add_done_callback(_on_completion)

    try:
        return await waiter
    except asyncio.CancelledError:
        for f in fs:
            f.cancel()
        await asyncio.wait(fs, loop=loop)
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
    Run awaitables concurrently and wait for them to finish.

    The ``exception_handling`` has three options \
    to control how exceptions are handled:

    ``RETURN_EXCEPTIONS`` will wait for \
    all tasks to finish and return everything as \
    values including exceptions.

    ``WAIT_TASKS_AND_RAISE`` will wait for \
    all tasks to finish and raise the first \
    exception afterwards.

    ``CANCEL_TASKS_AND_RAISE`` will raise the \
    first exception, cancel all remaining tasks \
    and wait for them to finish.

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

    results = RetList()
    if not aws:
        return results

    loop = loop or asyncio.get_event_loop()
    fs = [asyncio.ensure_future(aw, loop=loop) for aw in aws]
    cancel_on_first_ex = exception_handling == CANCEL_TASKS_AND_RAISE
    ffs = await _wait_completion(
        fs, loop=loop, cancel_on_first_ex=cancel_on_first_ex)

    # Raise chained exception preserving the raised order
    if exception_handling in (
            CANCEL_TASKS_AND_RAISE, WAIT_TASKS_AND_RAISE):
        exs = []
        should_cancel = False
        for f in ffs:
            if f.cancelled():
                should_cancel = True
                continue
            if f.exception():
                exs.append(f.exception())
        if exs:
            raise _chain_exceptions(exs)
        # Cancels have no traceback anyway
        if should_cancel:
            raise asyncio.CancelledError()

    for f in fs:
        if f.cancelled():
            results.append(asyncio.CancelledError())
            continue
        if f.exception():
            results.append(f.exception())
            continue
        if isinstance(f.result(), RetList):
            results.extend(f.result())
        else:
            results.append(f.result())
    return results


# XXX exception_handling
async def sequential(*aws, loop=None, return_exceptions=False):
    """
    Similar to ``concurrent`` except \
    the given tasks are ran sequentially.

    If ``return_exceptions`` is ``False`` (the default), \
    the first exception will be raised and no coro \
    will run further. Otherwise, all coros will run and \
    exceptions will be appended to the returned list.
    """
    loop = loop or asyncio.get_event_loop()
    results = RetList()
    cancel_all = False
    error = None
    for aw in aws:
        fut = asyncio.ensure_future(aw, loop=loop)

        if cancel_all:
            fut.cancel()
            await wait([fut], loop=loop)
            continue

        try:
            ret = await asyncio.shield(fut, loop=loop)
        except asyncio.CancelledError as err:
            if fut.cancelled():
                # Don't cancel sibling tasks
                if not return_exceptions:
                    error = err
                results.append(err)
            else:
                fut.cancel()
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


# XXX remove task list, use a set and remove task on_completed
# XXX make long stack traces optional. Creating a bazillion tasks that fail
#     create a huge traceback
class TaskGuard:
    """
    Create tasks and wait for them to finish.

    If a task raises an exception, then all remaining tasks \
    get cancelled and the first exception is raised. Same \
    thing if an exception gets raised within the ``with`` block.

    If more than one task raise an exception, they get chained.

    If a task is cancelled and decides to ignore cancellation, \
    the guard will still wait for it to finish.
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

        cancel_on_first_ex = True
        if exc_value is not None:
            for t in self._tasks:
                t.cancel()
            # Avoid cancelling again,
            # in case a task does cleanup on cancel
            cancel_on_first_ex = False

        ts = await _wait_completion(
            self._tasks, loop=self.loop, cancel_on_first_ex=cancel_on_first_ex)
        exs = []
        for t in ts:
            if t.cancelled():
                continue
            if t.exception():
                exs.append(t.exception())
        if exc_value is not None:
            exs.append(exc_value)
        if exs:
            raise _chain_exceptions(exs)

    def create_task(self, coro):
        if self._state == self._closed:
            raise ValueError("Guard is closed")
        task = self.loop.create_task(coro)
        self._tasks.append(task)
        return task
