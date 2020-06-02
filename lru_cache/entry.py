# coding: utf8

import time
import threading


class EntryStatus(object):
    CREATED  = 0b1
    UPDATING = 0b10
    UPDATED  = 0b100
    DELETING = 0b1000
    DELETED  = 0b10000


class Entry(object):
    def __init__(self, key=None):
        self._key = key
        self._ref_count = 0
        self._used_count = 0
        self._status = EntryStatus.CREATED
        self._expire = 0
        self._size = 0
        self._waiters = []

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    def incr_ref_count(self):
        self._ref_count = self._ref_count + 1

    def decr_ref_count(self):
        self._ref_count = max(0, self._ref_count - 1)

    def incr_used_count(self):
        self._used_count = self._used_count + 1

    @property
    def ref_count(self):
        return self._ref_count

    @property
    def used_count(self):
        return self._used_count

    @property
    def expire(self):
        return self._expire

    @expire.setter
    def expire(self, expire):
        self._expire = expire

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, size):
        self._size = size

    def mark_as_deleting(self):
        if self._status & EntryStatus.CREATED or \
                self._status & EntryStatus.UPDATED:
            self._status = EntryStatus.DELETING
            return True
        return False

    def mark_as_deleting_if_necessary(self):
        if self._status & EntryStatus.DELETING:
            return True
        return self.mark_as_deleting()

    def mark_as_deleted(self):
        if self._status & EntryStatus.DELETING:
            self._status = EntryStatus.DELETED
            return True
        return False

    def mark_as_updating(self):
        if self._status & EntryStatus.CREATED or \
                self._status & EntryStatus.DELETED:
            self._status = EntryStatus.UPDATING
            return True
        return False

    def set_updating_result(self, success):
        if not self._status & EntryStatus.UPDATING:
            return False
        self._status = EntryStatus.CREATED
        if success:
            self._status = EntryStatus.UPDATED
        # 唤醒所有等待更新完成的线程
        while self._waiters:
            waiter = self._waiters.pop(0)
            waiter.release()
        return True

    def wait_for_usable(self, lock, timeout):
        waiter = threading.Lock()
        waiter.acquire()
        self._waiters.append(waiter)
        lock.release()

        if timeout is None or timeout < 0:
            waiter.acquire()
            lock.acquire()
            return self._status & EntryStatus.UPDATED \
                and True or False

        end_time = time.time() + timeout
        delay = 0.0005  # 500 us -> initial delay of 1 ms
        while True:
            gotit = waiter.acquire(0)
            if gotit:
                break
            remaining = end_time - time.time()
            if remaining <= 0:
                break
            delay = min(delay * 2, remaining, .05)
            time.sleep(delay)

        lock.acquire()
        if not gotit:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
        return self._status & EntryStatus.UPDATED \
            and True or False

    def is_created(self):
        return self._status & EntryStatus.CREATED

    def is_updating(self):
        return self._status & EntryStatus.UPDATING

    def is_usable(self):
        return self._status & EntryStatus.UPDATED

    def is_deleting(self):
        return self._status & EntryStatus.DELETING

    def is_unusable(self):
        return self._status & EntryStatus.DELETING or \
            self._status & EntryStatus.DELETED
