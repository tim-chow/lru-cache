# coding: utf8

import random


class Node(object):
    def __init__(self, key=None, value=None, 
                 next=None, down=None):
        self._key = key
        self._value = value
        self._next = next
        self._down = down

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

    @property
    def down(self):
        return self._down

    @down.setter
    def down(self, down):
        self._down = down


class SkipListMap(object):
    """
    基于跳表的映射
    """
    def __init__(self):
        self._heads = [Node()]
        self._size = 0

    def __setitem__(self, key, value):
        try:
            self.__delitem__(key)
        except KeyError:
            pass

        k = 0
        for level in range(1, len(self._heads)+1):
            if random.random() < 0.5:
                break
            k = level
        else:
            self._heads.append(Node(down=self._heads[level-1]))

        node = self._heads[k]
        prev_new_node = None
        while node != None:
            while node.next != None and node.next.key < key:
                node = node.next
            new_node = Node(key, value)
            new_node.next = node.next
            if prev_new_node != None:
                prev_new_node.down = new_node
            prev_new_node = new_node
            node.next = new_node 
            node = node.down
        self._size = self._size + 1

    def __getitem__(self, key):
        node = self._heads[-1]
        while node != None:
            while node.next != None and node.next.key < key:
                node = node.next
            if node.next == None or node.next.key > key:
                node = node.down
                continue
            node = node.next
            while node.down != None:
                node = node.down
            return node.value
        raise KeyError(key)

    def __delitem__(self, key):
        exists = False
        node = self._heads[-1]
        while node != None:
            while node.next != None and node.next.key < key:
                node = node.next
            if node.next != None and node.next.key == key:
                exists = True
                node.next = node.next.next
            node = node.down
        if exists:
            self._size = self._size - 1
            # 删除空层
            while len(self._heads) > 1 and \
                    self._heads[-1].next == None:
                self._heads.pop(-1)
        else:
            raise KeyError(key)

    @property
    def size(self):
        return self._size

    @property
    def heads(self):
        return self._heads

def print_skiplist_map(slm):
    print("SkipListMap " + 
        "%r with %d elements " % (slm, slm.size) + 
        "is shown as below:")
    for level in range(len(slm.heads)-1, -1, -1):
        print("\tlevel %d:" % level)
        node = slm.heads[level]
        while node.next != None:
            node = node.next
            print("\t\tnode %r is shown as below:" % node)
            print("\t\t\tnode.key: %s" % node.key)
            print("\t\t\tnode.value: %s" % node.value)
            print("\t\t\tnode.next: %r" % node.next)
            print("\t\t\tnode.down: %r" % node.down)

def test_skiplist_map():
    elements = range(20) * 2
    random.shuffle(elements)
    print("elements are: \n\t%s" % elements)
    slm = SkipListMap()
    for element in elements:
        slm[element] = element
    assert slm.size == len(set(elements)), \
        "there are some bugs in SkipListMap.__setitem__(key, value)"
    for element in elements:
        assert slm[element] == element, \
            "there are some bugs in SkipListMap.__getitem__(key)"
    print_skiplist_map(slm)
    for element in set(elements):
        del slm[element]
    assert slm.size == 0, \
        "there are some bugs in SkipListMap.__delitem__(key)"

if __name__ == "__main__":
    test_skiplist_map()

