import weakref
from typing import List, Union
import functools
import contextlib
import time
from collections import OrderedDict
from enum import Enum
import gc


# def simple_lru(func=None, maxsize=128):
#     if func is None:
#         return functools.partial(simple_lru, maxsize=maxsize)
#
#     cache = OrderedDict()
#
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         keys = args
#         if kwargs:
#             keys += tuple(kwargs.items())
#
#         key = hash(keys)
#         if key in cache:
#             cache.move_to_end(key)
#             return cache[key]
#
#         result = func(*args, **kwargs)
#         if len(cache) == maxsize:
#             cache.popitem(last=False)
#         cache[key] = result
#         return result
#
#     return wrapper


class LiteralString:
    def __init__(self, value: str):
        self.value = value

    def to_bytes(self):
        return self.value.encode('ascii')

    def __str__(self):
        return self.value


def is_dim_function(
    func_name,
):
    return str(func_name) in (
        'cal::base',
        'cal::ibase',
        'cal::children',
        'cal::ichildren',
        'cal::descendant',
        'cal::idescendant',
    )


def simple_lru(
    func=None,
    maxsize=128,
    weakref_pos: Union[List[int], int] = None,
    weakref_key: Union[List[str], str] = None
):
    function_cache = weakref.WeakKeyDictionary()

    if weakref_pos is None:
        weakref_pos = []
    elif not isinstance(weakref_pos, list):
        weakref_pos = [weakref_pos]

    if weakref_key is None:
        weakref_key = []
    elif not isinstance(weakref_key, list):
        weakref_key = [weakref_key]

    if func is None:
        return functools.partial(
            simple_lru,
            maxsize=maxsize,
            weakref_pos=weakref_pos,
            weakref_key=weakref_key
        )

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        lru_kwargs = kwargs.copy()
        lru_args = list(args)

        valid_weakref_kwargs = []
        valid_weakref_args = []

        for ref_key in weakref_key:
            if ref_key in kwargs:
                lru_kwargs[ref_key] = weakref.ref(kwargs[ref_key])
                valid_weakref_kwargs.append(ref_key)

        arg_len = len(args)

        for pos in weakref_pos:
            if pos < arg_len:
                lru_args[pos] = weakref.ref(args[pos])
                valid_weakref_args.append(pos)

        if func in function_cache:
            cached_func = function_cache[func]

        else:
            @functools.wraps(func)
            @functools.lru_cache(maxsize=maxsize)
            def cached_func(*patched_args, **patched_kwargs):
                patched_args = list(patched_args)
                for idx in valid_weakref_args:
                    patched_args[idx] = patched_args[idx]()
                for key in valid_weakref_kwargs:
                    patched_kwargs[key] = patched_kwargs[key]()
                return func(*patched_args, **patched_kwargs)

            function_cache[func] = cached_func

        return cached_func(*lru_args, **lru_kwargs)

    return wrapper


# simple_lru = functools.lru_cache


class TimeUnit(str, Enum):
    ns = 'ns'
    us = 'us'
    ms = 'ms'
    s = 's'
    m = 'm'
    h = 'h'


TIME_MAP = {
    'ns': 1,
    'us': 1_000,
    'ms': 1_000_000,
    's': 1_000_000_000,
    'm': 60_000_000_000,
    'h': 3_600_000_000_000,
}


class Stopwatch(object):
    """计时器

    Args:
        unit: 计时单位
        sink: 耗时信息的输出函数，默认为 ``logger.info``

    >>> watch = Stopwatch(unit='s', sink=print)
    >>> import time
    >>> with watch('[task - sleep]'):
    ...    time.sleep(0)
    [task - sleep]:0.00s
    """
    __slots__ = (
        'runtimes', 'start_stack', 'rec_count', 'name',
        '_unit_repr', '_unit', '_sink', '_msg_on_enter'
    )

    def __init__(self, unit: str = 'ns', sink=print, msg_on_enter: bool = True):
        self.runtimes = OrderedDict()
        self.start_stack = []
        self.rec_count = 0
        self.name = []
        self._unit_repr = _unit = TimeUnit[unit]
        self._unit = TIME_MAP[_unit]
        self._sink = sink
        self._msg_on_enter = msg_on_enter

    def __call__(self, name=None):
        self.name.append(name)
        return self

    def __enter__(self):
        self.rec_count += 1
        if self._msg_on_enter:
            prefix, task_name = self.get_current_task_name(pop=False)
            self._sink(f'{prefix}entering: {task_name}')

        self.start_stack.append(time.perf_counter_ns())

    def __exit__(self, exc_type, exc_val, exc_tb):
        time_gap = time.perf_counter_ns() - self.start_stack.pop(-1)
        key = ''.join(self.get_current_task_name())
        self._sink(f"{key} takes: {time_gap / self._unit :.2f}{self._unit_repr}")
        self.runtimes[key] = time_gap

    def get_current_task_name(self, pop=True):
        stack_len = len(self.start_stack)
        prefix = '\t' * stack_len

        if self.name and self.name[-1] is not None:
            if pop:
                name = self.name.pop(-1)
            else:
                name = self.name[-1]
        else:
            name = f"task{self.rec_count}"

        return prefix, name

    def get_all_runtime(self):
        return list(self.runtimes.values())

    def clear(self):
        self.runtimes.clear()
        self.rec_count = 0

    def __repr__(self):
        return ', '.join(
            f"{name}:{t / self._unit :.2f}{self._unit_repr}"
            for name, t in self.runtimes.items()
        )


def stopwatch(func=None, unit: str = 'ms', name=None, use_global: bool = True):
    if func is None:
        return functools.partial(stopwatch, unit=unit, name=name, use_global=use_global)

    if use_global:
        watch = GlobalWatch
    else:
        watch = Stopwatch(unit)
    func_name = name or func.__qualname__

    @functools.wraps(func)
    def wrap(*args, **kwargs):
        with watch(func_name):
            rtn = func(*args, **kwargs)

        return rtn
    return wrap


class DummyStopwatch(Stopwatch):
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


GlobalWatch = DummyStopwatch(unit='ms')
# GlobalWatch = Stopwatch(unit='ms')


@contextlib.contextmanager
def disable_gc():
    gcold = gc.isenabled()
    try:
        gc.disable()
        yield
    finally:
        if gcold:
            gc.enable()
