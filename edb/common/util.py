import functools
from collections import OrderedDict


def simple_lru(func=None, maxsize=128):
    if func is None:
        return functools.partial(simple_lru, maxsize=maxsize)

    cache = OrderedDict()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        keys = args
        if kwargs:
            keys += tuple(kwargs.items())

        key = hash(keys)
        if key in cache:
            result = cache[key]
            cache.pop(key)
            cache[key] = result
            return result

        result = func(*args, **kwargs)
        if len(cache) == maxsize:
            cache.popitem(last=False)
        cache[key] = result
        return result

    return wrapper


class LiteralString:
    def __init__(self, value: str):
        self.value = value

    def to_bytes(self):
        return self.value.encode('ascii')

    def __str__(self):
        return self.value

