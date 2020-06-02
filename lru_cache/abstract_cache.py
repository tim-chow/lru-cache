# coding: utf8

import logging
import threading
import abc
from uuid import uuid1

LOGGER = logging.getLogger(__name__)


class CacheStatus(object):
    WAITING  = 0b1       # 等待载入 code：1
    STARTING = 0b10      # 正在启动 code：2
    LOADING  = 0b100     # 正在载入 code：4
    LOADED   = 0b1000    # 载入完毕 code：8
    STOPPING = 0b10000   # 正在停止 code：16
    STOPPED  = 0b100000  # 已经停止 code：32

    def __init__(self):
        self._condition = threading.Condition()
        self._current_status = self.WAITING

    def start(self, before_callback, after_callback):
        with self._condition:
            status = self._current_status
            if not status & self.WAITING and \
                    not status & self.STOPPED:
                return False
            self._current_status = self.STARTING

        try:
            before_callback()
        except:
            LOGGER.error(
                "fail to run before callback",
                exc_info=True)
            with self._condition:
                self._current_status = status
            raise
        else:
            with self._condition:
                self._current_status = self.LOADING

        try:
            after_callback()
        except:
            LOGGER.error(
                "fail to run after callback",
                exc_info=True)
        return True

    def transfer_to_loaded(self):
        with self._condition:
            if self._current_status & self.LOADING:
                self._current_status = self.LOADED
                self._condition.notify_all()
                return True
            return False

    def transfer_to_stopping(self):
        with self._condition:
            if self._current_status & self.LOADING or \
                    self._current_status & self.LOADED:
                self._current_status = self.STOPPING
                self._condition.notify_all()
                return True
            return False

    def transfer_to_stopped(self):
        with self._condition:
            if self._current_status & self.STOPPING:
                self._current_status = self.STOPPED
                self._condition.notify_all()
                return True
            return False

    def is_usable(self):
        with self._condition:
            return self._current_status & self.LOADED \
                and True or False

    def wait_for_usable(self, timeout):
        with self._condition:
            status = self._current_status
            if status & self.LOADED:
                return True
            if status & self.STOPPING or \
                    status & self.STOPPED:
                return False
            self._condition.wait(timeout)
            return self._current_status & self.LOADED \
                and True or False


class AbstractCache(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name=None):
        self._name = name or 'cache-%s' % uuid1().hex
        self._status = CacheStatus()
        self._manager_thread = None
        self._condition = threading.Condition()

    @property
    def name(self):
        return self._name

    def is_usable(self):
        return self._status.is_usable()

    def wait_for_usable(self, timeout=None):
        return self._status.wait_for_usable(timeout)

    def _start_before_callback(self):
        self.prepare()

        LOGGER.debug("begin to create manager "
                     "thread of %s" % self.name)
        self._manager_thread = threading.Thread(
            target=self.manager_thread_main)
        self._manager_thread.setName(
            "manager-thread-of-%s" % self.name)
        self._manager_thread.setDaemon(True)

    def _start_after_callback(self):
        self._manager_thread.start()
        LOGGER.debug("manager thread of " +
                     "%s is started" % self.name)

    def start(self):
        if not self._status.start(
                self._start_before_callback,
                self._start_after_callback):
            raise RuntimeError("fail to start %s" % self.name)

    @abc.abstractmethod
    def prepare(self):
        pass

    def manager_thread_main(self):
        if self.load_main():
            self.manage_main()

    def load_main(self):
        try:
            iterable = self.load()
            while True:
                if self._status.transfer_to_stopped():
                    LOGGER.info("manager thread of " + 
                                "%s exit" % self.name)
                    return False
                try:
                    wait_time = iterable.next()
                except StopIteration:
                    break
                if self._status.transfer_to_stopped():
                    LOGGER.info("manager thread of " + 
                                "%s exit" % self.name)
                    return False
                with self._condition:
                    self._condition.wait(wait_time)
        except:
            LOGGER.error(
                "%s failed to load cache",
                self.name,
                exc_info=True)
            self._status.transfer_to_stopping()
            self._status.transfer_to_stopped()
            raise

        self._status.transfer_to_loaded()
        return True

    @abc.abstractmethod
    def load(self):
        pass

    def manage_main(self):
        try:
            iterable = self.manage()
            while True:
                if self._status.transfer_to_stopped():
                    LOGGER.info("manager thread of " + 
                                "%s exit" % self.name)
                    return
                try:
                    wait_time = iterable.next()
                except StopIteration:
                    LOGGER.info("manager thread of " + 
                                "%s exit unexpectedly" % self.name)
                    self._status.transfer_to_stopping()
                    self._status.transfer_to_stopped()
                    return
                if self._status.transfer_to_stopped():
                    LOGGER.info("manager thread of " + 
                                "%s exit" % self.name)
                    return
                with self._condition:
                    self._condition.wait(wait_time)
        except:
            LOGGER.error(
                "%s failed to manage cache",
                self.name,
                exc_info=True)
            self._status.transfer_to_stopping()
            self._status.transfer_to_stopped()
            raise

    @abc.abstractmethod
    def manage(self):
        pass

    def stop(self, timeout=None):
        if not self._status.transfer_to_stopping():
            raise RuntimeError("fail to stop %s" % self.name)

        LOGGER.debug("begin to stop manager thread of %s",
                     self.name)
        if self._manager_thread is not None:
            with self._condition:
                self._condition.notify()
            self._manager_thread.join(timeout)
            if self._manager_thread.isAlive():
                LOGGER.error(
                    "manager thread of %s is still running",
                    self.name)
            else:
                LOGGER.debug(
                    "manager thread of %s is stopped",
                    self.name)
                self._manager_thread = None

        self.finalize()
        LOGGER.info("%s is stopped", self.name)

    @abc.abstractmethod
    def finalize(self):
        pass


def test_abstract_cache():
    import sys

    class Cache(AbstractCache):
        def __init__(self):
            AbstractCache.__init__(self, "test-cache")

        def load(self):
            for i in range(1, 11):
                wait_time = i * 0.1
                LOGGER.debug(
                    "load loop #%03d, wait %.3fs",

                    i,
                    wait_time)
                yield wait_time

        def manage(self):
            i = 0
            while True:
                i = i + 1
                wait_time = (i % 10) * 0.1
                LOGGER.debug(
                    "manage loop #%03d, wait %.3fs",
                    i,
                    wait_time)
                yield wait_time

        def prepare(self):
            LOGGER.debug("prepare() is called")

        def finalize(self):
            LOGGER.debug("finalize() is called")

    c = Cache()
    c.start()
    LOGGER.debug("press any key to exit")
    try:
        sys.stdin.read(1)
    finally:
        c.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(filename)s:%(lineno)d "
               "%(threadName)20s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    test_abstract_cache()
