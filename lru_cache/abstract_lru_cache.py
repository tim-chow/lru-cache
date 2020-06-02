# coding: utf8

import logging
import abc
import threading
import time
import sys
import hashlib
import functools

from .abstract_cache import AbstractCache
from .skiplist_map import SkipListMap
from .linked_queue import LinkedQueue
from .entry import Entry

LOGGER = logging.getLogger(__name__)


class Serializer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def loads(self, data):
        pass

    @abc.abstractmethod
    def dumps(self, obj):
        pass


class ProxyCache(object):
    def __init__(self):
        self._caches = []
        self._key_func = None
        self._call_func_when_failure = None
        self._serializer = None

    @property
    def caches(self):
        return self._caches

    def add_cache(self, cache):
        self._caches.append(cache)

    def set_key_func(self, key_func):
        assert callable(key_func)
        self._key_func = key_func

    def set_call_func_when_failure(self, value):
        self._call_func_when_failure = bool(value)

    def set_serializer(self, serializer):
        assert isinstance(serializer, Serializer)
        self._serializer = serializer

    def deco(self, func):
        if len(self._caches) == 0:
            raise RuntimeError("empty cache")
        if self._key_func is None:
            raise RuntimeError("missing key_func")
        if self._call_func_when_failure is None:
            raise RuntimeError("missing call_func_when_failure")
        if self._serializer is None:
            raise RuntimeError("missing serializer")

        @functools.wraps(func)
        def _inner(*args, **kwargs):
            key = self._key_func(func, *args, **kwargs)
            if isinstance(key, unicode):
                key = key.encode()
            if isinstance(key, str):
                key_md5 = hashlib.md5(key).hexdigest()
                index = int(key_md5, 16) % len(self._caches)
            elif isinstance(key, (int, long)):
                index = key % len(self._caches)
            else:
                raise TypeError("str or int expected")
            cache = self._caches[index]
            return cache.open(key, self._serializer,
                              self._call_func_when_failure, func,
                              *args, **kwargs)
        return _inner


class ReturnCode(object):
    ERROR_ENTRY_UNUSABLE     = 0b1
    ERROR_UNREACH_MIN_USES   = 0b10
    ERROR_WAIT_COUNT_REACHED = 0b100
    ERROR_CACHE_OVERFLOW     = 0b1000
    ERROR_KEY_NOT_EXISTS     = 0b10000
    ERROR_KEY_UPDATING       = 0b100000
    OK                       = 0b1000000
    RESPONSIBLE_FOR_UPDATING = 0b10000000


class CacheError(Exception):
    def __init__(self, code, *args):
        Exception.__init__(self, *args)
        self._code = code

    @property
    def code(self):
        return self._code


class AbstractLRUCache(AbstractCache):
    def __init__(self, name,
                 max_entry_count, max_size,
                 min_uses, max_inactive,
                 lock_age, wait_count,
                 expire_interval, forced_expire_interval):
        AbstractCache.__init__(self, name)
        self._lock = threading.Lock()
        self._map = SkipListMap()
        self._queue = LinkedQueue()

        self._max_entry_count = max_entry_count
        self._current_entry_count = 0
        self._max_size = max_size
        self._current_size = 0
        self._min_uses = min_uses
        self._max_inactive = max_inactive
        self._lock_age = lock_age
        self._wait_count = wait_count
        self._expire_interval = expire_interval
        self._forced_expire_interval = forced_expire_interval

    @staticmethod
    def _now():
        return time.time()

    def _is_full(self):
        return self._current_size >= self._max_size or \
            self._current_entry_count >= self._max_entry_count

    def add_meta(self, key, size):
        with self._lock:
            if self._current_entry_count >= self._max_entry_count:
                return False
            if self._current_size + size > self._max_size:
                return False
            entry = Entry(key)
            entry.size = size
            entry.expire = self._now() + self._max_inactive
            entry.mark_as_updating()
            entry.set_updating_result(True)
            node = self._queue.insert_to_head(entry)
            self._map[key] = node
            self._current_size = self._current_size + size
            self._current_entry_count = \
                self._current_entry_count + 1
            return True

    def _read_result_from_cache(self, key,
                                serializer, func,
                                *args, **kwargs):
        cached_data = None
        should_purge = False
        exc_info = None
        try:
            cached_data = self.read_cache(key)
        except KeyError:
            LOGGER.error(
                "meta of %s is " % key +
                "in LRU cache, but cached data is missing, " +
                "so purge it")
            should_purge = True
        except:
            exc_info = sys.exc_info()

        with self._lock:
            node = self._map[key]
            entry = node.data
            entry.decr_ref_count()

            if should_purge:
                entry.mark_as_deleting_if_necessary()

            if entry.is_deleting() and \
                    entry.ref_count == 0:
                self._delete_node_and_cache(node)

        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]
        if should_purge:
            return func(*args, **kwargs)
        return serializer.loads(cached_data)

    def _call_func_and_write_cache(
            self, key,
            serializer, func,
            *args, **kwargs):
        ret = None
        exc_info = None
        success = True
        size = None
        try:
            ret = func(*args, **kwargs)
        except:
            exc_info = sys.exc_info()
            success = False

        if success:
            try:
                size, data = serializer.dumps(ret)
                self.write_cache(key, data)
            except:
                exc_info = sys.exc_info()
                success = False

        with self._lock:
            node = self._map[key]
            entry = node.data
            entry.decr_ref_count()
            entry.set_updating_result(success)
            if success:
                entry.size = size
                self._current_size = self._current_size + size
        if success:
            return ret
        raise exc_info[0], exc_info[1], exc_info[2]

    def open(self, key, serializer,
             call_func_when_failure, func,
             *args, **kwargs):
        assert isinstance(serializer, Serializer)

        if not self.is_usable():
            LOGGER.debug("%s is not usable yet", self.name)
            cached_data = None
            try:
                cached_data = self.read_cache(key)
            except KeyError:
                return func(*args, **kwargs)
            return serializer.loads(cached_data)
        return self._open(key, serializer,
                          call_func_when_failure, func,
                          *args, **kwargs)

    def _open(self, key, serializer,
              call_func_when_failure, func,
              *args, **kwargs):
        rc = self._exists(key)
        if rc & ReturnCode.OK:
            return self._read_result_from_cache(
                        key,
                        serializer,
                        func,
                        *args,
                        **kwargs)
        elif rc & ReturnCode.RESPONSIBLE_FOR_UPDATING:
            return self._call_func_and_write_cache(
                        key,
                        serializer,
                        func,
                        *args,
                        **kwargs)
        elif rc & ReturnCode.ERROR_UNREACH_MIN_USES or \
                rc & ReturnCode.ERROR_ENTRY_UNUSABLE:
            return func(*args, **kwargs)
        else:
            if call_func_when_failure:
                return func(*args, **kwargs)
            raise CacheError(code=rc)

    def _key_exists(self, node):
        entry = node.data
        if entry.is_unusable():
            return ReturnCode.ERROR_ENTRY_UNUSABLE

        now = self._now()
        entry.incr_used_count()
        entry.expire = now + self._max_inactive
        self._queue.move_to_head(node)

        if entry.used_count < self._min_uses:
            return ReturnCode.ERROR_UNREACH_MIN_USES

        entry.incr_ref_count()

        if entry.is_usable():
            return ReturnCode.OK

        if entry.mark_as_updating():
            return ReturnCode.RESPONSIBLE_FOR_UPDATING

        for _ in range(self._wait_count):
            if entry.is_updating():
                LOGGER.debug(
                    "%s is updating, " % str(entry.key) +
                    "wait for it is usable")
                got_it = entry.wait_for_usable(
                    self._lock,
                    self._lock_age)
                if got_it:
                    return ReturnCode.OK
                if entry.mark_as_updating():
                    return ReturnCode.RESPONSIBLE_FOR_UPDATING
        else:
            LOGGER.debug("wait count is reached for %s",
                         entry.key)
            entry.decr_ref_count()
            if entry.is_deleting():
                if entry.ref_count == 0:
                    self._delete_node_and_cache(node)
                return ReturnCode.ERROR_ENTRY_UNUSABLE
            return ReturnCode.ERROR_WAIT_COUNT_REACHED

    def _exists(self, key):
        self._lock.acquire()
        try:
            node = self._map[key]
        except KeyError:
            if self._is_full():
                self._lock.release()
                self._forced_expire(20)
                self._lock.acquire()
            if self._is_full():
                self._lock.release()
                return ReturnCode.ERROR_CACHE_OVERFLOW
            entry = Entry(key)
            node = self._queue.insert_to_head(entry)
            self._map[key] = node
            self._current_entry_count = \
                self._current_entry_count + 1
        rc = self._key_exists(node)
        self._lock.release()
        return rc

    def _expire(self):
        self._lock.acquire()
        try:
            sentinel = None
            while True:
                node = self._queue.peek_last()
                if node is None:
                    break
                if node is sentinel:
                    break
                entry = node.data
                now = self._now()
                if entry.expire > now:
                    break
                if entry.ref_count == 0:
                    LOGGER.debug("%s is expired", entry.key)
                    self._delete_node_and_cache(node)
                    sentinel = None
                    continue

                LOGGER.debug(
                    "%s is referenced "
                    "by other threads, move it to the "
                    "head of lru queue",
                    entry)
                entry.expire = now + self._max_inactive
                self._queue.move_to_head(node)
                if sentinel is None:
                    sentinel = node
        finally:
            self._lock.release()

    def _forced_expire(self, tries=0):
        self._lock.acquire()
        sentinel = None
        try:
            while True:
                node = self._queue.peek_last()
                if node is None:
                    break
                if node is sentinel:
                    break
                entry = node.data
                if entry.ref_count == 0:
                    self._delete_node_and_cache(node)
                    return True

                entry.expire = self._now() + self._max_inactive
                self._queue.move_to_head(node)

                if sentinel is None:
                    sentinel = node

                if tries <= 0:
                    continue
                tries = tries - 1
                if tries == 0:
                    break
            return False
        finally:
            self._lock.release()

    def _delete_node_and_cache(self, node):
        entry = node.data
        if entry.ref_count > 0:
            LOGGER.error(
                "the referenced count "
                "of entry must be zero when "
                "calling this method")
            return
        if not entry.mark_as_deleting_if_necessary():
            LOGGER.error(
                "current status can not "
                "be transmitted to DELETING")
            return
        entry.incr_ref_count()
        self._lock.release()

        self.delete_cache(entry.key)

        self._lock.acquire()
        entry.decr_ref_count()
        entry.mark_as_deleted()
        self._current_size = \
            self._current_size - entry.size
        self._current_entry_count = \
            self._current_entry_count - 1
        del self._map[entry.key]
        self._queue.remove_node(node)

    @abc.abstractmethod
    def delete_cache(self, key):
        pass

    @abc.abstractmethod
    def read_cache(self, key):
        pass

    @abc.abstractmethod
    def write_cache(self, key, data):
        pass

    def manage(self):
        while True:
            self._expire()
            while True:
                self._lock.acquire()
                if self._current_size > self._max_size:
                    self._lock.release()
                    success = self._forced_expire()
                    if success:
                        continue
                    else:
                        yield self._forced_expire_interval
                        continue
                else:
                    self._lock.release()
                    break
            yield self._expire_interval

    def purge(self, key):
        with self._lock:
            try:
                node = self._map[key]
            except KeyError:
                return ReturnCode.ERROR_KEY_NOT_EXISTS

            entry = node.data
            if entry.is_unusable():
                return ReturnCode.OK

            if entry.is_updating():
                return ReturnCode.ERROR_KEY_UPDATING

            entry.mark_as_deleting()
            if entry.ref_count == 0:
                self._delete_node_and_cache(node)
            return ReturnCode.OK
