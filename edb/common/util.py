import functools
from collections import OrderedDict
import weakref
from typing import List, Union


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

