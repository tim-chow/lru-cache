# coding: utf8

import logging
import os.path
import sys

from lru_cache.file_lru_cache import FileLRUCacheBuilder
from lru_cache.abstract_lru_cache import (
    ProxyCache,
    Serializer,
    ReturnCode)

LOGGER = logging.getLogger(__name__)

# 测试之前，确保 BASE_DIR 存在
BASE_DIR = "data/"

if not os.path.isdir(BASE_DIR):
    sys.exit("please make sure BASE_DIR exists")


class TestSerializer(Serializer):
    def loads(self, data):
        return data

    def dumps(self, obj):
        return len(obj), obj


def test():
    flc = FileLRUCacheBuilder() \
        .with_name("file-lru-cache") \
        .with_base_path(BASE_DIR) \
        .with_max_entry_count(10000) \
        .with_max_size(10*1024*1024*1024) \
        .build()
    flc.start()
    flc.wait_for_usable()

    proxy_cache = ProxyCache()
    proxy_cache.add_cache(flc)
    proxy_cache.set_key_func(
        lambda _, key, *a, **kw: key)
    proxy_cache.set_call_func_when_failure(False)
    proxy_cache.set_serializer(TestSerializer())

    @proxy_cache.deco
    def f(key):
        return key

    try:
        k = "thisisavalidkey"
        for _ in range(2):
            ret = f(k)
            LOGGER.info("ret = [[%s]]", ret)
        for cache in proxy_cache.caches:
            ret = cache.purge(k)
            LOGGER.info(
                "purge result = [[%s]]",
                (ret & ReturnCode.OK and True or False))
    finally:
        for cache in proxy_cache.caches:
            cache.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(threadName)s "
               "%(filename)s:%(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    test()
