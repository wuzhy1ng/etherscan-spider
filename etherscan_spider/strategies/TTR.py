import logging


class TTR:
    name = 'TTR'

    def __init__(self, source, alpha: float = 0.15, beta: float = 0.8, epsilon=1e-5):
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
        if node == self.source and self.source not in self._vis:
            s = sum([e['value'] if e['from'] == self.source else 0 for e in edges])
            for e in edges:
                if e['from'] == self.source:
                    self.r[node][e['timeStamp'] - 0.001] = \
                        self.r[node].get(e['timeStamp'], 0) + e['value'] / s
                # elif e['to'] == self.source:
                #     self.r[node][e['timeStamp'] + 0.001] = \
                #         self.r[node].get(e['timeStamp'], 0) + e['value'] / s

        self._vis.add(node)

        # 拷贝一份residual vector，原有的清空
        r = self.r[node]
        self.r[node] = dict()

        # push过程
        self._self_push(node, r)
        yield from self._forward_push(node, edges, r)
        yield from self._backward_push(node, edges, r)

    def _self_push(self, node, r: dict):
        sum_r = 0
        for _, v in r.items():
            sum_r += v
        self.p[node] = self.p.get(node, 0) + self.alpha * sum_r

    def _forward_push(self, node, edges: list, r: dict):
        """
        将流动权重沿着时序递增的输出边传播，并返回传播的输出边
        :param node: 扩展节点
        :param edges: 扩展得到的边
        :return: 传播的输出边
        """
        # 取出所有输出的边和chip
        es_out = list()
        for e in edges:
            if e['from'] == node:
                es_out.append(e)
        r_node = [(t, v) for t, v in r.items()]

        # 根据时间排序-从小到大
        es_out.sort(key=lambda e: e['timeStamp'])
        r_node.sort(key=lambda c: c[0])

        # 累计叠加，计算每个chip之后的value之和
        j = len(es_out) - 1
        sum_w, W = 0, dict()
        for i in range(len(r_node) - 1, -1, -1):
            c = r_node[i]
            while j >= 0 and es_out[j]['timeStamp'] > c[0]:
                sum_w += es_out[j]['value']
                j -= 1
            W[c] = sum_w

        # 将流动传播给出度邻居
        j = 0
        d = 0
        for i in range(0, len(es_out)):
            e = es_out[i]
            while j < len(r_node) and e['timeStamp'] > r_node[j][0]:
                d += (r_node[j][1] / W[r_node[j]]) if W[r_node[j]] > 0 else 0
                j += 1

            if self.r.get(e['to']) is None:
                self.r[e['to']] = dict()
            inc = (1 - self.alpha) * self.beta * e['value'] * d
            self.r[e['to']][e['timeStamp']] = self.r[e['to']].get(e['timeStamp'], 0) + inc

            # 返回权重传播的输出边
            if inc > 0:
                yield e

        # 当流动权重碎片缺失输出边时将回流到自身
        while j < len(r_node):
            self.r[node][r_node[j][0]] = self.r[node].get(r_node[j][0], 0) + \
                                         (1 - self.alpha) * self.beta * r_node[j][1]
            j += 1
        # if len(es_out) == 0:
        #     for t, v in r.items():
        #         self.r[node][t] = self.r[node].get(t, 0) + (1 - self.alpha) * self.beta * v
        #     return []

    def _backward_push(self, node, edges: list, r: dict):
        """
        将流动权重沿着时序递增的输入边传播，并返回传播的输入边
        :param node: 扩展节点
        :param edges: 扩展得到的边
        :return: 传播的输入边
        """
        # 取出所有输出的边和chip
        es_in = list()
        for e in edges:
            if e['to'] == node:
                es_in.append(e)
        r_node = [(t, v) for t, v in r.items()]

        # 根据时间排序-从小到大
        es_in.sort(key=lambda e: e['timeStamp'])
        r_node.sort(key=lambda c: c[0])

        # 累计叠加，计算每个chip之后的value之和
        j = 0
        sum_w, W = 0, dict()
        for i in range(0, len(r_node)):
            c = r_node[i]
            while j < len(es_in) and es_in[j]['timeStamp'] < c[0]:
                sum_w += es_in[j]['value']
                j += 1
            W[c] = sum_w

        # 将流动传播给入度邻居
        j = len(r_node) - 1
        d = 0
        for i in range(len(es_in) - 1, -1, -1):
            e = es_in[i]
            while j >= 0 and e['timeStamp'] < r_node[j][0]:
                d += (r_node[j][1] / W[r_node[j]]) if W[r_node[j]] > 0 else 0
                j -= 1

            if self.r.get(e['from']) is None:
                self.r[e['from']] = dict()
            inc = (1 - self.alpha) * (1 - self.beta) * e['value'] * d
            self.r[e['from']][e['timeStamp']] = self.r[e['from']].get(e['timeStamp'], 0) + inc

            # 返回权重传播的输入边
            if inc > 0:
                yield e

        # 当流动权重碎片缺失输入边时将回流到自身
        while j >= 0:
            self.r[node][r_node[j][0]] = self.r[node].get(r_node[j][0], 0) + \
                                         (1 - self.alpha) * (1 - self.beta) * r_node[j][1]
            j -= 1
        # if len(es_in) == 0:
        #     for t, v in r.items():
        #         self.r[node][t] = self.r[node].get(t, 0) + (1 - self.alpha) * (1 - self.beta) * v
        #     return []

    def pop(self):
        nodes_r = list()
        for node, chips in self.r.items():
            # if node in self._vis:
            #     continue

            sum_r = 0
            for _, v in chips.items():
                sum_r += v
            nodes_r.append((node, sum_r))
            # if sum_r > self.epsilon:
            #     return node

        if len(nodes_r) > 0:
            nodes_r.sort(key=lambda x: x[1], reverse=True)
            for node, sum_r in nodes_r:
                if sum_r > self.epsilon:
                    logging.info(node + ' ' + str(sum_r))
                    return node

        return None
