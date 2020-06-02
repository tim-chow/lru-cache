import logging
import time
import threading

from lru_cache.abstract_lru_cache import (
    Serializer,
    AbstractLRUCache,
    ProxyCache,
    CacheError)

LOGGER = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(threadName)s "
           "%(filename)s:%(lineno)d %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

THREAD_COUNT = 100
LOOP_COUNT = 15000
CACHE_COUNT = 5
GROUP_COUNT = 20


class TestSerializer(Serializer):
    def loads(self, data):
        return data

    def dumps(self, obj):
        return 4, obj


class MemoryLRUCache(AbstractLRUCache):
    def __init__(self, *a, **kw):
        AbstractLRUCache.__init__(self, *a, **kw)

    def load(self):
        yield 0

    def prepare(self):
        self._d = {}

    def finalize(self):
        self._d = None

    def read_cache(self, key):
        return self._d[key]

    def write_cache(self, key, value):
        self._d[key] = value

    def delete_cache(self, key):
        self._d.pop(key, None)


proxy_cache = ProxyCache()
proxy_cache.set_serializer(TestSerializer())
proxy_cache.set_call_func_when_failure(False)
proxy_cache.set_key_func(lambda _, key, *a, **kw: key)

for cache_id in range(CACHE_COUNT):
    cache = MemoryLRUCache(
        name="memory-cache-%d" % cache_id,
        max_entry_count=10000,
        max_size=10*1024*1024*1024,
        max_inactive=3600,
        expire_interval=10,
        forced_expire_interval=2,
        min_uses=1,
        lock_age=2,
        wait_count=4)
    cache.start()
    cache.wait_for_usable()
    proxy_cache.add_cache(cache)


@proxy_cache.deco
def func(key):
    return key


def target(k):
    for _ in range(LOOP_COUNT):
        try:
            func(k)
        except CacheError:
            LOGGER.error(
                "fail to call func",
                exc_info=True)


def test(thread_count):
    threads = []
    for ind in range(thread_count):
        group_id = ind % GROUP_COUNT
        t = threading.Thread(target=target, args=(group_id, ))
        t.start()
        threads.append(t)

    start_time = time.time()
    for thread in threads:
        thread.join()
    time_used = time.time() - start_time
    LOGGER.debug("use %.3fs", time_used)
    LOGGER.debug("average: %fr/s",
                 (THREAD_COUNT*LOOP_COUNT/time_used))


if __name__ == "__main__":
    try:
        test(THREAD_COUNT)
    finally:
        for cache in proxy_cache.caches:
            cache.stop()
