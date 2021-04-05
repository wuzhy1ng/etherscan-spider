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
