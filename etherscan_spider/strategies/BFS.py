from queue import Queue


class BFS:
    name = 'BFS'

    def __init__(self, source):
        self._queue = Queue()
        self.vis = set()
        self.vis.add(source)

    def push(self, edges: list):
        for e in edges:
            if e.get('from') is not None:
                self._queue.put(e['from'])
            if e.get('to') is not None:
                self._queue.put(e['to'])

    def pop(self):
        while not self._queue.empty():
            node = self._queue.get()
            if node not in self.vis:
                self.vis.add(node)
                return node
        return None
