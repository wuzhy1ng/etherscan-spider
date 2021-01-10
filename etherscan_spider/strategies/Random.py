class Random:
    name = 'Random'

    def __init__(self, source):
        self._nodes = set()
        self._vis = set()
        self._vis.add(source)

    def push(self, edges: list):
        for e in edges:
            self._nodes.add(e.get('from'))
            self._nodes.add(e.get('to'))

    def pop(self):
        while len(self._nodes) != 0:
            node = self._nodes.pop()
            if not set(node).issubset(self._vis):
                self._vis.add(node)
                return node
        return None
