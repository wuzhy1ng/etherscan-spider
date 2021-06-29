class TTR:
    name = 'TTR'

    def __init__(self, source, alpha: float = 0.15, beta=0.8, epsilon=1e-5):
        self.source = source
        self.alpha = alpha
        self.beta = beta
        self.epsilon = epsilon

        self.p = dict()
        self.r = dict()
        self._vis = set()

    def push(self, node, edges: list):
        # residual vector空值判定
        if self.r.get(node) is None:
            self.r[node] = dict()

        # 当更新的是源节点时
        if node == self.source:
            in_es = list()
            for e in edges:
                if e['to'] == node:
                    in_es.append(e)

            in_sum = sum([e['value'] for e in in_es])
            for e in in_es:
                self.r[node][e['timeStamp']] = self.r[node].get(e['timeStamp'], 0) + \
                                               e['value'] / in_sum

        # push过程
        self._self_push(node)
        self._forward_push(node, edges)
        self._backward_push(node, edges)

        # push以后清空residual vector
        self.r[node] = dict()

    def _self_push(self, node):
        sum_r = 0
        for _, v in self.r[node].items():
            sum_r += v
        self.p[node] = self.p.get(node, 0) + self.alpha * sum_r

    def _forward_push(self, node, edges):
        # 取出所有输出的边和chip
        es_out = list()
        for e in edges:
            if e['from'] == node:
                es_out.append(e)
        r_node = [(t, v) for t, v in self.r[node].items()]

        # 根据时间排序-从小到大
        es_out.sort(key=lambda e: e['timeStamp'])
        r_node.sort(key=lambda c: c[0])

        # 累计叠加，计算每个chip之后的value之和
        j = len(es_out) - 1
        sum_w, W = 0, dict()
        for i in range(len(r_node) - 1, 0, -1):
            e = es_out[j]
            c = r_node[i]
            while j >= 0 and e['timeStamp'] > c[0]:
                sum_w += e['value']
                j -= 1
            W[c] = sum_w

        # 将流动传播给出度邻居
        j = 0
        d = 0
        for i in range(0, len(es_out)):
            e = es_out[i]
            c = r_node[j]
            while j < len(r_node) and e['timeStamp'] > c[0]:
                d += c[1] / W[c]
                j += 1

            if self.r.get(e['to']) is None:
                self.r[e['to']] = dict()
            self.r[e['to']][e['timeStamp']] = self.r[e['to']].get(e['timeStamp'], 0) + \
                                              (1 - self.alpha) * self.beta * e['value'] * d

    def _backward_push(self, node, edges):
        # 取出所有输出的边和chip
        es_in = list()
        for e in edges:
            if e['to'] == node:
                es_in.append(e)
        r_node = [(t, v) for t, v in self.r[node]]

        # 根据时间排序-从大到小
        es_in.sort(key=lambda e: e['timeStamp'], reverse=True)
        r_node.sort(key=lambda c: c[0], reverse=True)

        # 累计叠加，计算每个chip之后的value之和
        j = len(es_in) - 1
        sum_w, W = 0, dict()
        for i in range(len(r_node) - 1, 0, -1):
            e = es_in[j]
            c = r_node[i]
            while j >= 0 and e['timeStamp'] > c[0]:
                sum_w += e['value']
                j -= 1
            W[c] = sum_w

        # 将流动传播给入度邻居
        j = 0
        d = 0
        for i in range(0, len(es_in)):
            e = es_in[i]
            c = r_node[j]
            while j < len(r_node) and e['timeStamp'] > c[0]:
                d += c[0] / W[c]
                j += 1

            if self.r.get(e['from']) is None:
                self.r[e['from']] = dict()
            self.r[e['from']][e['timeStamp']] = self.r[e['from']].get(e['timeStamp'], 0) + \
                                                (1 - self.alpha) * (1 - self.beta) * e['value'] * d

    def pop(self):
        for node, chips in self.r:
            sum_r = 0
            for chip in chips:
                sum_r += chip[1]
            if sum_r > self.epsilon:
                return node
        return None
