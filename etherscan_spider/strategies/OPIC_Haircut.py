import networkx as nx


class OPICHaircut:
    def __init__(self, a: float = 0.8):
        self.a = a
        self.g = nx.MultiDiGraph()

    def push(self, sub_graph: nx.MultiDiGraph):
        """
        输入扩展某个节点后的子图，请确保输入是修改过的pop的子图
        :param sub_graph:扩展某个节点后的子图
        :return:
        """

        # 找到扩展点
        extended_node = None
        for k, v in sub_graph.nodes(data=True):
            if v.get('vis', False):
                extended_node = k
                break

        # 加入数据，这里要判定是否加入了重复边
        self.g.add_node(extended_node, vis=True)
        for _from, _to, attr in sub_graph.in_edges(extended_node, data=True):
            if self.g.nodes.get(_from) and self.g.nodes[_from].get('vis', False): continue
            self.g.add_edge(_from, _to, **attr)
        for _from, _to, attr in sub_graph.out_edges(extended_node, data=True):
            if self.g.nodes.get(_to) and self.g.nodes[_to].get('vis', False): continue
            self.g.add_edge(_from, _to, **attr)

        if not self.g.nodes[extended_node].get('dirty'):
            self.g.nodes[extended_node]['dirty'] = sub_graph.nodes[extended_node].get('dirty', 0)

        # 计算数据
        d = self.g.nodes[extended_node].get('dirty')
        sum_in = sum([e[2].get('weight', 0) for e in self.g.in_edges(extended_node, data=True)])
        sum_out = sum([e[2].get('weight', 0) for e in self.g.out_edges(extended_node, data=True)])
        if sum_in == 0 or sum_out == 0: return
        R = sum_out / sum_in if sum_out / sum_in < 1 else 1

        for e in self.g.in_edges(extended_node, data=True):
            self.g.nodes[e[0]]['dirty'] = self.g.nodes[e[0]].get('dirty', 0) + \
                                          (1 - self.a) * (e[2].get('weight', 0) / sum_in) * d * R
        for e in self.g.out_edges(extended_node, data=True):
            self.g.nodes[e[1]]['dirty'] = self.g.nodes[e[1]].get('dirty', 0) + \
                                          self.a * (e[2].get('weight', 0) / sum_out) * d * R

    def pop(self):
        """
        输出权重最大的、未被扩展过的的节点
        :return:
        """
        nodes = list()
        for k, attr in self.g.nodes.items():
            if attr.get('vis', False): continue
            nodes.append((k, attr.get('dirty', 0)))

        if len(nodes) == 0: return None
        nodes.sort(key=lambda node: node[1], reverse=True)  # 按权值排序
        self.g.nodes[nodes[0][0]]['vis'] = True  # 扩展标记

        # 新建图
        sub_graph = nx.MultiDiGraph()
        sub_graph.add_node(nodes[0][0], vis=True, dirty=nodes[0][1])
        return sub_graph
