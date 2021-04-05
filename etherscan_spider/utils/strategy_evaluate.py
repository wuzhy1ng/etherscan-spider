import csv
import os
import sys
import time

import networkx as nx
from matplotlib import pyplot as plt


class StrategyEvaluator:
    def __init__(self, observer_labels: list, logs_dir='./logs/', log_name=None, log_interval=100):
        self.edges = set()
        self.nodes = set()
        self.observer_labels = observer_labels
        self.observer_label_nodes = {label: set() for label in observer_labels}
        self.other_label_nodes = set()

        self.logs_dir = logs_dir
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        self.log_name = log_name if log_name is not None else str(int(time.time()))
        self.log_interval = log_interval
        self._extend_cnt = 0

        self.label_map = dict()
        with open('./data/labeled_address.csv') as f:
            for row in csv.reader(f):
                self.label_map[row[0]] = row[1]

    def update_state(self, tx_list):

        # 更新记录状态
        for tx in tx_list:
            self.edges.add(tx.get('hash'))
            self.nodes.add(tx.get('from'))
            self.nodes.add(tx.get('to'))

            if self.label_map.get(tx.get('from')) is not None:
                label = self.label_map[tx.get('from')]
                if label in self.observer_labels:
                    self.observer_label_nodes[label].add(tx.get('from'))
                else:
                    self.other_label_nodes.add(tx.get('from'))

            if self.label_map.get(tx.get('to')) is not None:
                label = self.label_map[tx.get('to')]
                if label in self.observer_labels:
                    self.observer_label_nodes[label].add(tx.get('to'))
                else:
                    self.other_label_nodes.add(tx.get('to'))

        # 达到·计数间隔时保存状态
        self._extend_cnt += 1
        if self._extend_cnt % self.log_interval == 0:
            self._summary_log()

    def _summary_log(self):
        log_filename = os.path.join(self.logs_dir, self.log_name + '.csv')
        if not os.path.exists(log_filename):
            with open(log_filename, 'w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['extend_cnt', 'edges', 'nodes', *self.observer_labels, 'other_label'])
        with open(log_filename, 'a', newline='') as f:
            w = csv.writer(f)
            w.writerow([
                self._extend_cnt, len(self.edges), len(self.nodes),
                *[len(self.observer_label_nodes[label]) for label in self.observer_labels],
                len(self.other_label_nodes)
            ])


if __name__ == '__main__':
    logs_dir = 'H:\\etherscan_spider\\data\\tmp\\logs\\'
    observer_labels = ['phish-hack']

    # 加载log数据
    data = dict()
    for filename in os.listdir(logs_dir):
        legend_name = filename.split('.')[0]
        with open(logs_dir + filename, 'r') as f:
            r = csv.reader(f)
            keys = next(r)
            data[legend_name] = {key: list() for key in keys}
            for row in r:
                for i, item in enumerate(row):
                    data[legend_name][keys[i]].append(int(item))

    # 计算对齐需要的截断长度
    truncate_len = sys.maxsize
    for summary in data.values():
        truncate_len = min(truncate_len, len(summary['extend_cnt']))

    # 绘制节点数量变化曲线
    plt.title('nodes')
    legend_names = list()
    for legend_name, summary in data.items():
        plt.plot(summary['nodes'][:truncate_len])
        legend_names.append(legend_name)
    plt.legend(legend_names)
    plt.show()

    # 绘制各种标签节点数量的变化曲线
    labels = set()
    for summary in data.values():
        for key in summary.keys():
            labels.add(key)
    labels.remove('extend_cnt')
    labels.remove('nodes')
    labels.remove('edges')

    for label in labels:
        plt.title(label)
        legend_names = list()
        for legend_name, summary in data.items():
            plt.plot([summary[label][i] for i in range(truncate_len)])
            legend_names.append(legend_name)
        plt.ylabel('labeled address count')
        plt.xlabel('extend count, unit:100')
        plt.legend(legend_names)
        plt.show()

    # 收集所有种子名称
    seeds = set()
    with open('H:\\etherscan_spider\\data\\seed.csv', 'r') as f:
        for row in csv.reader(f):
            seeds.add(row[0].lower())

    # label map
    label_map = dict()
    with open('H:\\etherscan_spider\\data\\labeled_address.csv') as f:
        for row in csv.reader(f):
            label_map[row[0]] = row[1]

    # observer_label节点平均度
    # 子图直径
    bar_metrics = {legend_name: dict() for legend_name in data.keys()}
    for legend_name in data.keys():
        for seed in seeds:
            print(legend_name, seed)
            g = nx.Graph()
            path = os.path.join('H:\\etherscan_spider\\data\\tmp\\', legend_name, '300_' + seed + '.csv')
            if not os.path.exists(path):
                continue
            with open(path, 'r') as f:
                for row in csv.reader(f):
                    g.add_edge(row[1], row[2])

            # observer_label节点平均度
            for node in g.nodes:
                label = label_map.get(node)
                if label in observer_labels:
                    bar_metrics[legend_name][label] = \
                        bar_metrics[legend_name].get(label, 0) + g.degree(node)
                    bar_metrics[legend_name][label + '_cnt'] = \
                        bar_metrics[legend_name].get(label + '_cnt', 0) + 1

            # 子图直径
            tmp = nx.shortest_path_length(g, seed)
            max_dep = max(list(tmp.values()))
            bar_metrics[legend_name]['diameter'] = bar_metrics[legend_name].get('diameter', 0) + max_dep
            bar_metrics[legend_name]['diameter_cnt'] = bar_metrics[legend_name].get('diameter_cnt', 0) + 1

    for k, v in bar_metrics.items():
        keys = set(list(v.keys()))
        for key in keys:
            if key + '_cnt' in keys:
                v[key] /= v[key + '_cnt']
        bar_metrics[k] = v
    print(bar_metrics)
