# coding: utf8

import random

P = 0.25
MAX_LEVEL = 32


class Node(object):
    def __init__(self,
                 key=None,
                 next=None,
                 down=None):
        self._key = key
        self._next = next
        self._down = down

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def next(self):
        return self._next

    @next.setter
    def next(self, next):
        self._next = next

    @property
    def down(self):
        return self._down

    @down.setter
    def down(self, down):
        self._down = down


class DataNode(object):
    def __init__(self,
                 key=None,
                 value=None,
                 next=None):
        self._key = key
        self._value = value
        self._next = next

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def next(self):
        return self._next

    @next.setter
    def next(self, next):
        self._next = next


class SkipListMap(object):
    """基于跳表的映射"""
    def __init__(self):
        self._heads = [DataNode()]
        self._size = 0

    def __setitem__(self, key, value):
        # 判断在哪些层插入
        k = 0
        for level in range(1, len(self._heads)+1):
            if random.random() < (1 - P):
                break
            k = level
        else:
            if len(self._heads) == MAX_LEVEL:
                k = len(self._heads) - 1
            else:
                self._heads.append(Node(down=self._heads[k-1]))

        node = self._heads[k]
        prev_new_node = None
        while True:
            while node.next is not None and node.next.key < key:
                node = node.next
            if node.next is None or node.next.key > key:
                # 插入新节点
                is_zero_level = isinstance(node, DataNode)
                if is_zero_level:
                    new_node = DataNode(key, value)
                else:
                    new_node = Node(key)
                new_node.next = node.next
                node.next = new_node
                if prev_new_node is not None:
                    prev_new_node.down = new_node
                prev_new_node = new_node
                if is_zero_level:
                    self._size = self._size + 1
                    break
                node = node.down
            else:
                node = node.next
                if prev_new_node is not None:
                    prev_new_node.down = node
                while not isinstance(node, DataNode):
                    node = node.down
                node.value = value
                break

    def __getitem__(self, key):
        node = self._heads[-1]
        while True:
            while node.next is not None and node.next.key < key:
                node = node.next
            if node.next is None or node.next.key > key:
                if isinstance(node, DataNode):
                    break
                node = node.down
                continue
            node = node.next
            while not isinstance(node, DataNode):
                node = node.down
            return node.value
        raise KeyError(key)

    def __delitem__(self, key):
        exists = False
        node = self._heads[-1]
        while True:
            while node.next is not None and node.next.key < key:
                node = node.next
            if node.next is not None and node.next.key == key:
                exists = True
                node.next = node.next.next
            if isinstance(node, DataNode):
                break
            node = node.down
        if exists:
            self._size = self._size - 1

            while len(self._heads) > 1:
                if self._heads[-1].next is not None:
                    break
                self._heads.pop(-1)
        else:
            raise KeyError(key)

    def __len__(self):
        return self._size

    @property
    def heads(self):
        return self._heads

    @property
    def level(self):
        return len(self._heads)


def print_skip_list_map(slm):
    print("SkipListMap %r with %d elements is shown as below:" %
          (slm, len(slm)))
    for level in range(len(slm.heads)-1, -1, -1):
        print("\tlevel %d:" % level)
        node = slm.heads[level]
        while node.next is not None:
            node = node.next
            print("\t\tnode %r is shown as below:" % node)
            print("\t\t\tnode.key: %s" % node.key)
            print("\t\t\tnode.next: %r" % node.next)
            if isinstance(node, DataNode):
                print("\t\t\tnode.value: %r" % node.value)
            else:
                print("\t\t\tnode.down: %r" % node.down)


def test_skip_list_map(map_obj, element_count, repeat_count):
    import time
    import pprint

    elements = range(element_count) * repeat_count
    random.shuffle(elements)
    print("elements are:")
    # pprint.pprint(elements)

    start_time = time.time()

    for element in elements:
        map_obj[element] = element

    assert len(map_obj) == len(set(elements)), \
        "there are some bugs in __setitem__(key, value)"
    for element in elements:
        assert map_obj[element] == element, \
            "there are some bugs in __getitem__(key)"
    for element in set(elements):
        del map_obj[element]
    assert len(map_obj) == 0, \
        "there are some bugs in __delitem__(key)"

    print("time elapsed %.3fs" % (time.time() - start_time))


if __name__ == "__main__":
    test_skip_list_map(SkipListMap(), 1000, 2)
