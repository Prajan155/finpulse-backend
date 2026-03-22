from __future__ import annotations

import time
import threading
from functools import wraps
from typing import Any, Callable


class TTLCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None

            expires_at, value = item
            if now >= expires_at:
                self._store.pop(key, None)
                return None

            return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        expires_at = time.time() + ttl_seconds
        with self._lock:
            self._store[key] = (expires_at, value)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


cache = TTLCache()


def _make_cache_key(prefix: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    parts = [prefix]

    if args:
        parts.append("args=" + repr(args))

    if kwargs:
        ordered_kwargs = sorted(kwargs.items(), key=lambda x: x[0])
        parts.append("kwargs=" + repr(ordered_kwargs))

    return "|".join(parts)


def ttl_cache(prefix: str, ttl_seconds: int) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = _make_cache_key(prefix, args, kwargs)
            cached = cache.get(key)
            if cached is not None:
                return cached

            value = func(*args, **kwargs)
            cache.set(key, value, ttl_seconds)
            return value

        return wrapper

    return decorator