# coding: utf8

import logging
import os.path
import os
import shutil
import uuid
import errno

from .abstract_lru_cache import AbstractLRUCache

LOGGER = logging.getLogger(__name__)


class FileLRUCache(AbstractLRUCache):
    def __init__(self, base_path, levels, 
            load_max_files, load_interval, 
            *args, **kwargs):
        AbstractLRUCache.__init__(self, *args, **kwargs)
        self._temp_file_prefix = "tempfile"
        self._base_path = base_path
        self._levels = self._generate_levels(levels)
        self._load_max_files = load_max_files
        self._load_interval = load_interval

    def _generate_path(self, key, only_dir_part=False):
        dir_names = []
        end = len(key)
        for level in self._levels:
            start = end - level
            dir_names.append(key[start:end])
            end = start
        if not only_dir_part:
            dir_names.append(key)
        return os.path.join(self._base_path, *dir_names)

    def _generate_levels(self, levels):
        levels = levels.split(":")
        if len(levels) > 3:
            levels = levels[:3]
        try:
            if len(levels) < 1:
                raise ValueError
            levels = map(int, levels)
            for level in levels:
                if level > 2:
                    raise ValueError
        except ValueError:
            levels = [1, 2]
        return levels

    def _safe_remove_dir(self, path):
        try:
            shutil.rmtree(path)
        except (IOError, OSError):
            LOGGER.error("fail to remove %s" % path, exc_info=True)

    def _safe_remove_file(self, path):
        try:
            os.remove(path)
        except (IOError, OSError):
            LOGGER.error("fail to remove %s" % path, exc_info=True)

    def _is_valid_dir_name(self, name, length):
        if len(name) != length:
            return False
        for char in name:
            if not char.isalnum():
                return False
        return True

    def _walk(self, base_directory, level, max_levels):
        if not os.path.isdir(base_directory):
            return
        if level < 1:
            raise RuntimeError("level must be more than 1")
        if level <= max_levels:
            for name in os.listdir(base_directory):
                path = os.path.join(base_directory, name)
                if os.path.isfile(path):
                    self._safe_remove_file(path)
                    continue
                if not self._is_valid_dir_name(
                        name, self._levels[level-1]):
                    self._safe_remove_dir(path)
                    continue
                for file_path, name, size in \
                        self._walk(path, level+1, max_levels):
                    yield file_path, name, size
        elif level == max_levels + 1:
            for name in os.listdir(base_directory):
                path = os.path.join(base_directory, name)
                if os.path.isdir(path):
                    self._safe_remove_dir(path)
                    continue
                if name.startswith(self._temp_file_prefix):
                    self._safe_remove_file(path)
                    continue
                if self._generate_path(name) != path:
                    self._safe_remove_file(path)
                    continue
                try:
                    size = os.stat(path).st_size
                except (IOError, OSError):
                    LOGGER.error("fail to stat %s" % path)
                    continue
                yield path, name, size

    def load(self):
        """
        加载缓存。该过程中会清理临时文件和不合法的目录、文件
        """
        count = 0
        for file_path, name, size in \
                self._walk(self._base_path, 1, len(self._levels)):
            if self.add_meta(name, size):
                LOGGER.debug("add meta for %s" % name)
            else:
                LOGGER.debug("fail to add meta for %s" % name)
                self._safe_remove_file(file_path)
            count = count + 1
            if count >= self._load_max_files:
                LOGGER.debug("load_max_files reached, "
                    "wait for %fs" % self._load_interval)
                yield self._load_interval
                count = 0

    def write_cache(self, key, data):
        if not self._is_valid_key(key):
            message = "invalid key %s" % key
            LOGGER.error(message)
            raise RuntimeError(message) 

        LOGGER.debug("write cache for %s" % key)
        dir_part = self._generate_path(key, True)
        path = os.path.join(dir_part, key)
        temp_path = os.path.join(dir_part,
            "%s-%s-%s" % (self._temp_file_prefix, 
                          key,
                          uuid.uuid1().hex))
        # 确保各层目录存在
        try:
            os.makedirs(dir_part)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        LOGGER.debug("write temporary file %s" % temp_path)
        with open(temp_path, "wb") as fd:
            fd.write(data)
        LOGGER.debug("rename %s to %s" % (temp_path, path))
        os.rename(temp_path, path)

    def read_cache(self, key):
        if not self._is_valid_key(key):
            message = "invalid key %s" % key
            LOGGER.error(message)
            raise RuntimeError(message) 

        LOGGER.debug("read cache for %s" % key)
        path = self._generate_path(key)
        if not os.path.isfile(path):
            LOGGER.error("%s is not file" % path)
            raise KeyError(key)
        if not os.access(path, os.R_OK):
            LOGGER.error("permission denied for %s" % path)
            raise KeyError(key)
        try:
            with open(path) as fd:
                return fd.read()
        except (IOError, OSError):
            LOGGER.error("fail to read %s" % path, exc_info=True)
        raise KeyError(key)

    def delete_cache(self, key):
        if not self._is_valid_key(key):
            LOGGER.error("invalid key %s" % key)
            return

        LOGGER.debug("delete cache for %s" % key)
        path = self._generate_path(key)
        if not os.path.isfile(path):
            LOGGER.error("%s is not file" % path)
            return
        if not os.access(path, os.W_OK):
            LOGGER.error("permission denied for %s" % path)
            return

        self._safe_remove_file(path)

    def _is_valid_key(self, key):
        if isinstance(key, unicode):
            key = key.encode()
        if not isinstance(key, str):
            return False
        if not key.isalnum():
            return False
        if len(key) < sum(self._levels):
            return False
        return True

    def prepare(self):
        LOGGER.debug("preparing FileLRUCache")

    def finalize(self):
        LOGGER.debug("FileLRUCache is finalized")


class FileLRUCacheBuilder(object):
    def __init__(self):
        self._name = None
        self._base_path = None
        self._levels = "1:2"
        self._load_max_files = 10000
        self._load_interval = 0.01
        self._max_entry_count = None
        self._max_size = None
        self._min_uses = 1
        self._max_inactive = 24 * 60 * 60
        self._lock_age = 0.4
        self._wait_count = 5
        self._expire_interval = 10
        self._forced_expire_interval = 1

    def with_name(self, name):
        self._name = name
        return self

    def with_base_path(self, base_path):
        self._base_path = base_path
        return self

    def with_levels(self, levels):
        self._levels = levels
        return self

    def with_load_max_files(self, load_max_files):
        self._load_max_files = load_max_files
        return self

    def with_load_interval(self, load_interval):
        self._load_interval = load_interval
        return self

    def with_max_entry_count(self, max_entry_count):
        self._max_entry_count = max_entry_count
        return self

    def with_max_size(self, max_size):
        self._max_size = max_size
        return self

    def with_min_uses(self, min_uses):
        self._min_uses = min_uses
        return self

    def with_max_inactive(self, max_inactive):
        self._max_inactive = max_inactive
        return self

    def with_lock_age(self, lock_age):
        self._lock_age = lock_age
        return self

    def with_wait_count(self, wait_count):
        self._wait_count = wait_count
        return self

    def with_expire_interval(self, expire_interval):
        self._expire_interval = expire_interval
        return self

    def with_forced_expire_interval(self, 
            forced_expire_interval):
        self._forced_expire_interval = \
            forced_expire_interval
        return self

    def build(self):
        if self._base_path == None:
            raise RuntimeError("missing base_path")
        if self._levels == None:
            raise RuntimeError("missing levels")
        if self._load_max_files == None:
            raise RuntimeError("missing load_max_files")
        if self._load_interval == None:
            raise RuntimeError("missing load_interval")
        if self._max_entry_count == None:
            raise RuntimeError("missing max_entry_count")
        if self._max_size == None:
            raise RuntimeError("missing max_size")
        if self._min_uses == None:
            raise RuntimeError("missing min_uses")
        if self._max_inactive == None:
            raise RuntimeError("missing max_inactive")
        if self._lock_age == None:
            raise RuntimeError("missing lock_age")
        if self._wait_count == None:
            raise RuntimeError("missing wait_count")
        if self._expire_interval == None:
            raise RuntimeError("missing expire_interval")
        if self._forced_expire_interval == None:
            raise RuntimeError("missing forced_expire_interval")

        return FileLRUCache(
            self._base_path,
            self._levels,
            self._load_max_files,
            self._load_interval,
            self._name,
            self._max_entry_count,
            self._max_size,
            self._min_uses,
            self._max_inactive,
            self._lock_age,
            self._wait_count,
            self._expire_interval,
            self._forced_expire_interval)

