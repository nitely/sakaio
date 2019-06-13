# -*- coding: utf-8 -*-

import asyncio
import logging

import asynctest
import unittest

import sakaio
from sakaio.sakaio import _chain_exceptions


class WasteException(Exception):
    pass


class SomeOtherException(Exception):
    pass


async def waste_cycles(cycles):
    for _ in range(cycles):
        await asyncio.sleep(0)


def call_later(cycles, callback):
    async def _call_later():
        await waste_cycles(cycles)
        callback()

    loop = asyncio.get_event_loop()
    return loop.create_task(_call_later())


# XXX make public?
class Waster:
    """Waste event loop cycles"""
    def __init__(self):
        self.completion_order = []
        self.cancelled = []

    async def waste(self, name, *, cycles, raise_err=False, ignore_cancel=False):
        """
        Waste cycles, record execution order,
        raise some exception and ignore cancellation
        """
        try:
            await waste_cycles(cycles)
        except asyncio.CancelledError:
            if not ignore_cancel:
                self.cancelled.append(name)
                raise
            await waste_cycles(cycles)
        if raise_err:
            raise WasteException(str(name))
        self.completion_order.append(name)
        return name


class ChainExceptionsTest(unittest.TestCase):

    def test_chain(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
        try:
            raise SomeOtherException('B')
        except SomeOtherException as err:
            ex2 = err
        err = _chain_exceptions([ex1, ex2])
        self.assertEqual(err, ex2)
        self.assertEqual(err.__context__, ex1)
        self.assertIsNone(err.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_nested(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
            try:
                raise SomeOtherException('B')
            except SomeOtherException as err:
                ex2 = err
        try:
            raise SomeOtherException('C')
        except SomeOtherException as err:
            ex3 = err
        err = _chain_exceptions([ex2, ex3])
        self.assertEqual(err, ex3)
        self.assertEqual(err.__context__, ex2)
        self.assertEqual(err.__context__.__context__, ex1)
        self.assertIsNone(err.__context__.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_from(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
            try:
                raise SomeOtherException('B') from ex1
            except SomeOtherException as err:
                ex2 = err
        err = _chain_exceptions([ex2])
        self.assertEqual(err, ex2)
        self.assertEqual(err.__context__, ex1)
        self.assertIsNone(err.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_simple(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
        err = _chain_exceptions([ex1, ex1])
        self.assertEqual(err, ex1)
        self.assertIsNone(err.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_dups(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
        try:
            raise SomeOtherException('B')
        except SomeOtherException as err:
            ex2 = err
        err = _chain_exceptions([ex1, ex2, ex1, ex2])
        self.assertEqual(err, ex2)
        self.assertEqual(err.__context__, ex1)
        self.assertIsNone(err.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_dups_nested(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
            try:
                raise SomeOtherException('B')
            except SomeOtherException as err:
                ex2 = err
        err = _chain_exceptions([ex1, ex2, ex1, ex2])
        self.assertEqual(err, ex2)
        self.assertEqual(err.__context__, ex1)
        self.assertIsNone(err.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err

    def test_chain_dups_from(self):
        try:
            raise SomeOtherException('A')
        except SomeOtherException as err:
            ex1 = err
            try:
                raise SomeOtherException('B') from ex1
            except SomeOtherException as err:
                ex2 = err
        err = _chain_exceptions([ex2, ex2])
        self.assertEqual(err, ex2)
        self.assertEqual(err.__cause__, ex1)
        self.assertEqual(err.__context__, ex1)
        self.assertIsNone(err.__context__.__context__)
        with self.assertRaises(SomeOtherException):
            raise err


class ConcurrentSequentialTest(asynctest.TestCase):

    def setUp(self):
        self.waster = Waster()

    async def test_concurrent(self):
        """
        Should run the coros concurrently and\
        return the results in the coros order
        """
        results = await sakaio.concurrent(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'B', 'E', 'A', 'C', 'D'])

    async def test_sequential(self):
        """
        Should run the coros sequentially and\
        return the results in the coros order
        """
        results = await sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'A', 'B', 'C', 'D', 'E'])

    async def test_sequential_concurrent(self):
        """
        Should run coros sequentially/concurrently as given
        """
        results = await sakaio.concurrent(
            self.waster.waste("A", cycles=30),
            self.waster.waste("B", cycles=20),
            sakaio.sequential(
                self.waster.waste("C", cycles=40),
                sakaio.concurrent(
                    self.waster.waste("D", cycles=50),
                    self.waster.waste("E", cycles=20)),
                self.waster.waste("F", cycles=10)))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E', 'F'])
        self.assertEqual(self.waster.completion_order, [
            'B', 'A', 'C', 'E', 'D', 'F'])

    async def test_concurrent_err(self):
        with self.assertRaises(WasteException) as cm:
            await sakaio.concurrent(
                self.waster.waste('Z', cycles=300, raise_err=True, ignore_cancel=True),
                self.waster.waste('X', cycles=200, ignore_cancel=True),
                self.waster.waste('A', cycles=20),
                self.waster.waste('B', cycles=10),
                self.waster.waste('C', cycles=30, raise_err=True),
                self.waster.waste('D', cycles=40),  # won't run
                self.waster.waste('E', cycles=15))
        self.assertIsInstance(cm.exception, WasteException)
        self.assertEqual(str(cm.exception), "Z")
        self.assertIsInstance(cm.exception.__context__, WasteException)
        self.assertEqual(str(cm.exception.__context__), "C")
        self.assertIsNone(cm.exception.__context__.__context__)
        self.assertEqual(self.waster.completion_order, [
            'B', 'E', 'A', 'X'])

    async def test_concurrent_err_ret(self):
        results = await sakaio.concurrent(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30, raise_err=True),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            exception_handling=sakaio.RETURN_EXCEPTIONS)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertTrue(isinstance(results[2], WasteException))
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'B', 'E', 'A', 'D'])

    async def test_concurrent_cancel_inner(self):
        c_task = self.loop.create_task(
            self.waster.waste('C', cycles=30))
        call_later(cycles=15, callback=c_task.cancel)

        task = self.loop.create_task(sakaio.concurrent(
            self.waster.waste('X', cycles=200, ignore_cancel=True),
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            c_task,
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15)))
        # Same behaviour as asyncio.gather
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['B', 'E', 'A', 'D', 'X'])

    async def test_concurrent_cancel_inner_ret(self):
        c_task = self.loop.create_task(
            self.waster.waste('C', cycles=30))
        call_later(cycles=15, callback=c_task.cancel)

        results = await sakaio.concurrent(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            c_task,
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            exception_handling=sakaio.RETURN_EXCEPTIONS)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertIsInstance(results[2], asyncio.CancelledError)
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'B', 'E', 'A', 'D'])

    async def test_concurrent_cancel_outer(self):
        task = self.loop.create_task(sakaio.concurrent(
            self.waster.waste('X', cycles=200, ignore_cancel=True),
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15)))
        call_later(cycles=5, callback=task.cancel)
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['X'])

    async def test_concurrent_cancel_outer_ret(self):
        task = self.loop.create_task(sakaio.concurrent(
            self.waster.waste('X', cycles=200, ignore_cancel=True),
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            exception_handling=sakaio.RETURN_EXCEPTIONS))
        call_later(cycles=5, callback=task.cancel)
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['X'])

    async def test_sequential_err(self):
        with self.assertRaises(WasteException):
            await sakaio.sequential(
                self.waster.waste('A', cycles=20),
                self.waster.waste('B', cycles=10),
                self.waster.waste('C', cycles=30, raise_err=True),
                self.waster.waste('D', cycles=40),  # won't run
                self.waster.waste('E', cycles=15))  # won't run
        await waste_cycles(200)
        self.assertEqual(self.waster.completion_order, [
            'A', 'B'])

    async def test_sequential_err_ret(self):
        results = await sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30, raise_err=True),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            return_exceptions=True)
        await waste_cycles(200)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertIsInstance(results[2], WasteException)
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'A', 'B', 'D', 'E'])

    async def test_sequential_cancel_inner(self):
        c_task = self.loop.create_task(
            self.waster.waste('C', cycles=30))

        task = self.loop.create_task(sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            c_task,
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15)))
        await waste_cycles(12)
        c_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['A', 'B', 'D', 'E'])

    async def test_sequential_cancel_inner_ret(self):
        c_task = self.loop.create_task(
            self.waster.waste('C', cycles=30))

        task = sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            c_task,
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            return_exceptions=True)
        await waste_cycles(5)
        c_task.cancel()
        results = await task
        await waste_cycles(200)
        self.assertEqual(len(results), 5)
        self.assertEqual(results[:2], [
            'A', 'B'])
        print(results[2])
        self.assertIsInstance(results[2], asyncio.CancelledError)
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(self.waster.completion_order, [
            'A', 'B', 'D', 'E'])

    async def test_sequential_cancel_outer(self):
        task = self.loop.create_task(sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15)))
        await waste_cycles(5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await waste_cycles(200)
        self.assertEqual(self.waster.completion_order, [])

    async def test_sequential_cancel_outer_ret(self):
        task = self.loop.create_task(sakaio.sequential(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            self.waster.waste('C', cycles=30),
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            return_exceptions=True))
        await waste_cycles(5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await waste_cycles(200)
        self.assertEqual(self.waster.completion_order, [])

    async def test_gather_behaviour(self):
        task_c = self.loop.create_task(self.waster.waste('C', cycles=30))
        call_later(cycles=10, callback=task_c.cancel)

        async def gather():
            await asyncio.gather(
                self.waster.waste('A', cycles=20),
                self.waster.waste('B', cycles=10),
                task_c,
                self.waster.waste('D', cycles=40),
                self.waster.waste('E', cycles=15))

        task = self.loop.create_task(gather())
        with self.assertRaises(asyncio.CancelledError):
            await task
        await waste_cycles(200)
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['B', 'E', 'A', 'D'])

    async def test_gather_behaviour_ret(self):
        task_c = self.loop.create_task(self.waster.waste('C', cycles=30))

        task = asyncio.gather(
            self.waster.waste('A', cycles=20),
            self.waster.waste('B', cycles=10),
            task_c,
            self.waster.waste('D', cycles=40),
            self.waster.waste('E', cycles=15),
            return_exceptions=True)
        await waste_cycles(5)
        task_c.cancel()
        result = await task
        await waste_cycles(200)
        self.assertEqual(result[:2], ['A', 'B'])
        self.assertIsInstance(result[2], asyncio.CancelledError)
        self.assertEqual(result[3:], ['D', 'E'])
        self.assertEqual(self.waster.completion_order, ['B', 'E', 'A', 'D'])


class TaskGuardTest(asynctest.TestCase):

    def setUp(self):
        self.waster = Waster()

    async def test_guard(self):
        loop = asyncio.get_event_loop()
        async with sakaio.TaskGuard(loop) as guard:
            guard.create_task(self.waster.waste('C', cycles=30))
            guard.create_task(self.waster.waste('A', cycles=10))
            guard.create_task(self.waster.waste('B', cycles=20))

        self.assertEqual(self.waster.completion_order, ['A', 'B', 'C'])

    async def test_guard_err(self):
        loop = asyncio.get_event_loop()
        with self.assertRaises(WasteException):
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(self.waster.waste('D', cycles=200, ignore_cancel=True))
                guard.create_task(self.waster.waste('C', cycles=30))  # will get cancelled
                guard.create_task(self.waster.waste('A', cycles=10))
                guard.create_task(self.waster.waste('B', cycles=20, raise_err=True))

        self.assertEqual(self.waster.completion_order, ['A', 'D'])

    async def test_guard_err_outside_err(self):
        loop = asyncio.get_event_loop()
        with self.assertRaises(WasteException):
            async with sakaio.TaskGuard(loop) as guard:
                # all get cancelled
                guard.create_task(self.waster.waste('D', cycles=200, ignore_cancel=True))
                guard.create_task(self.waster.waste('C', cycles=30))
                guard.create_task(self.waster.waste('A', cycles=10))
                guard.create_task(self.waster.waste('B', cycles=20))
                await waste_cycles(1)
                raise WasteException()

        self.assertEqual(self.waster.completion_order, ['D'])

    async def test_guard_err_inside_err(self):
        loop = asyncio.get_event_loop()
        with self.assertRaises(WasteException) as cm:
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(self.waster.waste('D', cycles=200, ignore_cancel=True))
                guard.create_task(self.waster.waste('C', cycles=30))
                guard.create_task(self.waster.waste('A', cycles=10))
                t = guard.create_task(self.waster.waste('B', cycles=20, raise_err=True))
                await t  # raises
                raise SomeOtherException("should not raise this")

        self.assertIsNone(cm.exception.__context__)  # No SomeOtherException
        self.assertEqual(self.waster.completion_order, ['A', 'D'])

    async def test_guard_err_mixed_err(self):
        loop = asyncio.get_event_loop()
        with self.assertRaises(SomeOtherException) as cm:
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(self.waster.waste('D', cycles=200, ignore_cancel=True))
                guard.create_task(self.waster.waste('C', cycles=30))  # gets cancelled
                guard.create_task(self.waster.waste('A', cycles=10))
                t = guard.create_task(self.waster.waste('B', cycles=20, raise_err=True))
                await asyncio.wait([t], loop=loop)
                raise SomeOtherException()  # gets raised as well
        self.assertIsInstance(cm.exception.__context__, WasteException)
        self.assertEqual(self.waster.completion_order, ['A', 'D'])

    async def test_guard_cancel_some_task(self):
        loop = asyncio.get_event_loop()
        async with sakaio.TaskGuard(loop) as guard:
            guard.create_task(self.waster.waste('C', cycles=30))
            guard.create_task(self.waster.waste('A', cycles=10))
            t = guard.create_task(self.waster.waste('B', cycles=20))
            t.cancel()
        self.assertEqual(self.waster.completion_order, ['A', 'C'])

    async def test_guard_cancel_outer_task(self):
        async def task_guard():
            loop = asyncio.get_event_loop()
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(self.waster.waste('D', cycles=200, ignore_cancel=True))
                guard.create_task(self.waster.waste('C', cycles=30))
                guard.create_task(self.waster.waste('A', cycles=10))
                guard.create_task(self.waster.waste('B', cycles=20))

        task = self.loop.create_task(task_guard())
        call_later(cycles=15, callback=task.cancel)
        await asyncio.wait([task], loop=self.loop)
        self.assertTrue(task.cancelled())
        self.assertEqual(self.waster.completion_order, ['A', 'D'])


class CancelAllTasksTest(asynctest.TestCase):

    def setUp(self):
        self.waster = Waster()

    async def test_cancel_all_tasks(self):
        t1 = asyncio.create_task(self.waster.waste('A', cycles=100))
        t2 = asyncio.create_task(self.waster.waste('B', cycles=200))
        assert t1 and t2  # shut up lint
        await waste_cycles(10)
        await sakaio.cancel_all_tasks(raise_timeout_error=True)
        self.assertEqual(self.waster.cancelled, ['A', 'B'])
        self.assertFalse(self.waster.completion_order)

    async def test_cancel_all_tasks_timeout(self):
        terminate = False
        async def never_ending():
            nonlocal terminate
            while True:
                try:
                    await waste_cycles(1)
                except asyncio.CancelledError:
                    pass
                if terminate:
                    return

        t1 = asyncio.create_task(never_ending())
        await waste_cycles(10)
        with self.assertRaises(asyncio.TimeoutError), \
             self.assertLogs('sakaio', logging.WARNING):
            await sakaio.cancel_all_tasks(timeout=0, raise_timeout_error=True)
        terminate = True
        await t1


if __name__ == '__main__':
    asynctest.main()
