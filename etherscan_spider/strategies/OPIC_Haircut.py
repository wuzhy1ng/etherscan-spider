class OPICHaircut:
    name = 'OPICHaircut'

    def __init__(self, source, dirty: float = 1.0, a: float = 0.8):
        self.a = a
        self._dirty = dict()
        self._vis = set()

        self._dirty[source] = dirty
        self._vis.add(source)

    def push(self, seed, edges: list):
        sum_in = 0
        sum_out = 0
        for e in edges:
            if seed == e.get('from'):
                sum_out += e.get('value', 0)
            elif seed == e.get('to'):
                sum_in += e.get('value', 0)
        if sum_in == 0 or sum_out == 0: return
        R = sum_out / sum_in if sum_out / sum_in < 1 else 1

        d = self._dirty.get(seed, 0)
        for e in edges:
            if seed == e.get('from'):
                _to = e.get('to')
                self._dirty[_to] = self._dirty.get(_to, 0) + self.a * (e.get('value', 0) / sum_out) * d * R
            elif seed == e.get('to'):
                _from = e.get('from')
                self._dirty[_from] = self._dirty.get(_from, 0) + (1 - self.a) * (e.get('value', 0) / sum_in) * d * R

    def pop(self):
        items = list(self._dirty.items())
        items.sort(key=lambda x: x[1], reverse=True)
        for item in items:
            if item[0] not in self._vis:
                self._vis.add(item[0])
                return item[0]
