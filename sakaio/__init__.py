# -*- coding: utf-8 -*-

from sakaio.sakaio import (
    cancel_all_tasks,
    RetList,
    concurrent,
    sequential,
    RETURN_EXCEPTIONS,
    CANCEL_TASKS_AND_RAISE,
    WAIT_TASKS_AND_RAISE,
    TaskGuard,
    wait,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    ALL_COMPLETED)

__all__ = [
    'cancel_all_tasks',
    'RetList',
    'concurrent',
    'sequential',
    'TaskGuard',
    'RETURN_EXCEPTIONS',
    'CANCEL_TASKS_AND_RAISE',
    'WAIT_TASKS_AND_RAISE',
    'wait',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
    'ALL_COMPLETED']

__version__ = '3.0-dev'
