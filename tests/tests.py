# -*- coding: utf-8 -*-

import asyncio

import asynctest

import sakaio


class SomeException(Exception):
    pass


class SomeOtherException(Exception):
    pass


async def waste_cycles(cycles):
    for _ in range(cycles):
        await asyncio.sleep(0)


async def do_stuff(ret, on_done, cycles, raise_err=False):
    await waste_cycles(cycles)
    if raise_err:
        raise SomeException(str(ret))
    on_done(ret)
    return ret


class ConcurrentSequentialTest(asynctest.TestCase):

    async def test_concurrent(self):
        """
        Should run the coros concurrently and\
        return the results in the coros order
        """
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        results = await sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E'])
        self.assertEqual(ret_order, [
            'B', 'E', 'A', 'C', 'D'])

    async def test_sequential(self):
        """
        Should run the coros sequentially and\
        return the results in the coros order
        """
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        results = await sakaio.sequential(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E'])
        self.assertEqual(ret_order, [
            'A', 'B', 'C', 'D', 'E'])

    async def test_sequential_concurrent(self):
        """
        Should run coros sequentially/concurrently as given
        """
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        results = await sakaio.concurrent(
            do_stuff("A", on_done, cycles=30),
            do_stuff("B", on_done, cycles=20),
            sakaio.sequential(
                do_stuff("C", on_done, cycles=40),
                sakaio.concurrent(
                    do_stuff("D", on_done, cycles=50),
                    do_stuff("E", on_done, cycles=20)),
                do_stuff("F", on_done, cycles=10)))
        self.assertEqual(results, [
            'A', 'B', 'C', 'D', 'E', 'F'])
        self.assertEqual(ret_order, [
            'B', 'A', 'C', 'E', 'D', 'F'])

    async def test_concurrent_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            if ret == 'D':
                print('ERROR IN test_concurrent_err')
            ret_order.append(ret)

        with self.assertRaises(SomeException):
            await sakaio.concurrent(
                do_stuff('A', on_done, cycles=20),
                do_stuff('B', on_done, cycles=10),
                do_stuff('C', on_done, cycles=30, raise_err=True),
                do_stuff('D', on_done, cycles=40),  # won't run
                do_stuff('E', on_done, cycles=15))
        self.assertEqual(ret_order, [
            'B', 'E', 'A'])

    async def test_concurrent_err_ret(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        results = await sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30, raise_err=True),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15),
            return_exceptions=True)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertTrue(isinstance(results[2], SomeException))
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(ret_order, [
            'B', 'E', 'A', 'D'])

    async def test_concurrent_cancel_inner(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order, c_task
            ret_order.append(ret)

        c_task = self.loop.create_task(
            do_stuff('C', on_done, cycles=30))

        task = self.loop.create_task(sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            c_task,
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15)))
        await waste_cycles(12)
        c_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # So... gather() docs are wrong here.
        # Cancelling a task, cancels all tasks :(
        # I can work around that, but I want the
        # same behaviour as gather (even if buggy). But do I?
        self.assertEqual(ret_order, ['B'])

    async def test_concurrent_cancel_inner_ret(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order, c_task
            if ret == 'B':
                c_task.cancel()
            ret_order.append(ret)

        c_task = self.loop.create_task(
            do_stuff('C', on_done, cycles=30))

        results = await sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            c_task,
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15),
            return_exceptions=True)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertTrue(isinstance(results[2], asyncio.CancelledError))
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(ret_order, [
            'B', 'E', 'A', 'D'])

    async def test_concurrent_cancel_outer(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        task = self.loop.create_task(sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15)))
        await waste_cycles(5)
        task.cancel()
        self.assertEqual(ret_order, [])

    async def test_concurrent_cancel_outer_ret(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        task = self.loop.create_task(sakaio.concurrent(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15),
            return_exceptions=True))
        await waste_cycles(5)
        task.cancel()
        self.assertEqual(ret_order, [])

    async def test_sequential_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            if ret in ('D', 'E'):
                print('ERROR IN test_sequential_err')
            ret_order.append(ret)

        with self.assertRaises(SomeException):
            await sakaio.sequential(
                do_stuff('A', on_done, cycles=20),
                do_stuff('B', on_done, cycles=10),
                do_stuff('C', on_done, cycles=30, raise_err=True),
                do_stuff('D', on_done, cycles=40),  # won't run
                do_stuff('E', on_done, cycles=15))  # won't run
        self.assertEqual(ret_order, [
            'A', 'B'])

    async def test_sequential_err_ret(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        results = await sakaio.sequential(
            do_stuff('A', on_done, cycles=20),
            do_stuff('B', on_done, cycles=10),
            do_stuff('C', on_done, cycles=30, raise_err=True),
            do_stuff('D', on_done, cycles=40),
            do_stuff('E', on_done, cycles=15),
            return_exceptions=True)
        self.assertEqual(results[:2], [
            'A', 'B'])
        self.assertTrue(isinstance(results[2], SomeException))
        self.assertEqual(results[3:], [
            'D', 'E'])
        self.assertEqual(ret_order, [
            'A', 'B', 'D', 'E'])


class TaskGuardTest(asynctest.TestCase):

    async def test_guard(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        async with sakaio.TaskGuard(loop) as guard:
            guard.create_task(do_stuff('C', on_done, cycles=30))
            guard.create_task(do_stuff('A', on_done, cycles=10))
            guard.create_task(do_stuff('B', on_done, cycles=20))

        self.assertEqual(ret_order, ['A', 'B', 'C'])

    async def test_guard_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        with self.assertRaises(SomeException):
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(do_stuff('C', on_done, cycles=30))  # will get cancelled
                guard.create_task(do_stuff('A', on_done, cycles=10))
                guard.create_task(do_stuff('B', on_done, cycles=20, raise_err=True))

        self.assertEqual(ret_order, ['A'])

    async def test_guard_err_outside_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        with self.assertRaises(SomeException):
            async with sakaio.TaskGuard(loop) as guard:
                # all get cancelled
                guard.create_task(do_stuff('C', on_done, cycles=30))
                guard.create_task(do_stuff('A', on_done, cycles=10))
                guard.create_task(do_stuff('B', on_done, cycles=20))
                raise SomeException()

        self.assertEqual(ret_order, [])

    async def test_guard_err_inside_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        with self.assertRaises(SomeException) as cm:
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(do_stuff('C', on_done, cycles=30))
                guard.create_task(do_stuff('A', on_done, cycles=10))
                t = guard.create_task(do_stuff('B', on_done, cycles=20, raise_err=True))
                await t  # raises
                raise SomeOtherException("should not raise this")

        self.assertIsNone(cm.exception.__context__)  # No SomeOtherException
        self.assertEqual(ret_order, ['A'])

    async def test_guard_err_mixed_err(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        with self.assertRaises(SomeException) as cm:
            async with sakaio.TaskGuard(loop) as guard:
                guard.create_task(do_stuff('C', on_done, cycles=30))  # gets cancelled
                guard.create_task(do_stuff('A', on_done, cycles=10))
                t = guard.create_task(do_stuff('B', on_done, cycles=20, raise_err=True))
                await asyncio.wait([t], loop=loop)
                raise SomeOtherException()  # gets raised as well

        self.assertIsInstance(cm.exception.__context__, SomeOtherException)
        self.assertEqual(ret_order, ['A'])

    async def test_guard_cancel_some_task(self):
        ret_order = []

        def on_done(ret):
            nonlocal ret_order
            ret_order.append(ret)

        loop = asyncio.get_event_loop()
        async with sakaio.TaskGuard(loop) as guard:
            guard.create_task(do_stuff('C', on_done, cycles=30))
            guard.create_task(do_stuff('A', on_done, cycles=10))
            t = guard.create_task(do_stuff('B', on_done, cycles=20))
            t.cancel()

        self.assertEqual(ret_order, ['A', 'C'])


if __name__ == '__main__':
    asynctest.main()
