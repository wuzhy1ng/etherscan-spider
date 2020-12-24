from queue import Queue

import networkx as nx


class BFS:
    def __init__(self):
        self._queue = Queue()
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
            self._queue.put(_from)
        for _from, _to, attr in sub_graph.out_edges(extended_node, data=True):
            if self.g.nodes.get(_to) and self.g.nodes[_to].get('vis', False): continue
            self.g.add_edge(_from, _to, **attr)
            self._queue.put(_to)

    def pop(self) -> nx.MultiDiGraph:
        """
        未被扩展过的的节点
        :return:
        """
        while True:
            if self._queue.qsize() == 0:
                return None
            node = self._queue.get()
            if not self.g.nodes[node].get('vis', False):
                self.g.nodes[node]['vis'] = True  # 扩展标记

                # 新建图
                sub_graph = nx.MultiDiGraph()
                sub_graph.add_node(node, vis=True)
                return sub_graph
