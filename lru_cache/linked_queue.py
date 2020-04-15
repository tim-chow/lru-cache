# coding: utf8


class Node(object):
    def __init__(self, data=None,
                 prev=None, next=None):
        self._data = data
        self._prev = prev
        self._next = next

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def prev(self):
        return self._prev

    @prev.setter
    def prev(self, prev):
        self._prev = prev

    @property
    def next(self):
        return self._next

    @next.setter
    def next(self, next):
        self._next = next


class LinkedQueue(object):
    """
    双向循环队列
    """
    def __init__(self):
        self._head = Node()
        self._head.prev = self._head
        self._head.next = self._head
        self._size = 0

    def insert_to_head(self, data):
        inserted_node = Node(data)
        head_next = self._head.next
        self._head.next = inserted_node
        head_next.prev = inserted_node
        inserted_node.prev = self._head
        inserted_node.next = head_next
        self._size = self._size + 1
        return inserted_node

    def insert_to_last(self, data):
        inserted_node = Node(data)
        current_last = self._head.prev
        current_last.next = inserted_node
        self._head.prev = inserted_node
        inserted_node.prev = current_last
        inserted_node.next = self._head
        self._size = self._size + 1
        return inserted_node

    def move_to_head(self, node):
        node_prev = node.prev
        node_next = node.next
        node_prev.next = node_next
        node_next.prev = node_prev
        head_next = self._head.next
        head_next.prev = node
        self._head.next = node
        node.prev = self._head
        node.next = head_next

    def peek_last(self):
        head_prev = self._head.prev
        if head_prev is self._head:
            return None
        return head_prev

    def remove_last(self):
        last = self._head.prev
        if last is self._head:
            return None
        last_prev = last.prev
        last_prev.next = self._head
        self._head.prev = last_prev
        self._size = self._size - 1
        last.prev = None
        last.next = None
        return last

    def remove_node(self, node):
        node_prev = node.prev
        node_next = node.next
        node_prev.next = node_next
        node_next.prev = node_prev
        node.prev = None
        node.next = None

    def get_prev_node(self, node):
        node_prev = node.prev
        if node_prev is self._head:
            return None
        return node_prev

    def iter(self):
        node = self._head
        while node.next is not self._head:
            yield node.next.data
            node = node.next

    @property
    def size(self):
        return self._size

def test_linked_queue():
    lq = LinkedQueue()
    assert list(lq.iter()) == [] and lq.size == 0
    lq.insert_to_head(1)
    assert list(lq.iter()) == [1] and lq.size == 1
    lq.insert_to_last(2)
    assert list(lq.iter()) == [1, 2] and lq.size == 2
    lq.insert_to_head(3)
    assert list(lq.iter()) == [3, 1, 2] and lq.size == 3

    last = lq.peek_last()
    assert last.data == 2
    lq.move_to_head(last)
    assert list(lq.iter()) == [2, 3, 1]
    lq.move_to_head(lq.peek_last())
    assert list(lq.iter()) == [1, 2, 3]
    lq.move_to_head(lq.peek_last())
    assert list(lq.iter()) == [3, 1, 2]

    assert lq.peek_last().data == 2
    assert lq.remove_last().data == 2
    assert lq.remove_last().data == 1
    assert lq.remove_last().data == 3
    assert lq.remove_last() == None
    assert lq.size == 0

    print("all tests passed")

if __name__ == "__main__":
    test_linked_queue()

