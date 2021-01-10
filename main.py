import csv
import json
import os

import networkx as nx

from etherscan_spider.strategies import Random, OPICHaircut

label_map = dict()
with open('./data/labeled_address.csv', 'r') as f:
    for row in csv.reader(f):
        label_map[row[0]] = row[1]


def init_source_graph(filename):
    g = nx.MultiDiGraph()
    with open(filename, 'r') as f:
        for row in csv.reader(f):
            g.add_edge(row[1], row[2], weight=row[3])
    for node in g.nodes:
        if label_map.get(node):
            g.nodes[node]['label'] = label_map.get(node)
    return g


def get_phishing_cnt(g: nx.MultiDiGraph):
    cnt = 0
    for node in g.nodes(data=True):
        if node.get('label') == 'phish-hack':
            cnt += 1
    return cnt


def get_phishing_rate(g: nx.MultiDiGraph):
    return get_phishing_cnt(g) / g.number_of_nodes()


def get_phishing_degree_avg(g: nx.MultiDiGraph):
    degree_list = []
    for node in g.nodes(data=True):
        if node.get('label') == 'phish-hack':
            degree_list.append(g.degree(node))
    return sum(degree_list) / len(degree_list)


def get_exended_phishing_cnt(g: nx.MultiDiGraph):
    cnt = 0
    for node in g.nodes(data=True):
        if node.get('label') == 'phish-hack' and node.get('vis'):
            cnt += 1
    return cnt


def get_exended_phishing_rate(g: nx.MultiDiGraph):
    cnt = 0
    for node in g.nodes(data=True):
        if node.get('vis'):
            cnt += 1
    return get_exended_phishing_cnt(g) / cnt


def gen_phishing_plot_data(seed, strategy, epoch: int, step: int, source: nx.MultiDiGraph):
    s = strategy()
    init_g = nx.MultiDiGraph()
    init_g.add_node(seed, vis=True, dirty=sum([e[2].get('weight', 0) for e in source.in_edges(seed, data=True)]))
    init_g.add_weighted_edges_from([(e[0], e[1], e[2].get('weight', 0)) for e in source.in_edges(seed, data=True)])
    init_g.add_weighted_edges_from([(e[0], e[1], e[2].get('weight', 0)) for e in source.out_edges(seed, data=True)])
    s.push(init_g)

    anchor = [(i + 1) * step for i in range(int(epoch / step))]
    anchor_cnt = len(anchor)
    pcnt = [0 for i in range(anchor_cnt)]
    prate = [0 for i in range(anchor_cnt)]
    pdavg = [0 for i in range(anchor_cnt)]
    epcnt = [0 for i in range(anchor_cnt)]
    eprate = [0 for i in range(anchor_cnt)]

    for i in range(epoch):
        if i in anchor:
            pcnt[anchor.index(i)] = get_phishing_cnt(s.g)
            prate[anchor.index(i)] = get_phishing_rate(s.g)
            pdavg[anchor.index(i)] = get_phishing_degree_avg(s.g)
            epcnt[anchor.index(i)] = get_exended_phishing_cnt(s.g)
            eprate[anchor.index(i)] = get_exended_phishing_rate(s.g)

        while True:
            g = s.pop()
            if g is None: break
            seed = list(g.nodes)[0]
            g.add_weighted_edges_from([(e[0], e[1], e[2].get('weight', 0)) for e in source.in_edges(seed, data=True)])
            g.add_weighted_edges_from([(e[0], e[1], e[2].get('weight', 0)) for e in source.out_edges(seed, data=True)])
            s.push(g)
            break

    return pcnt, prate, pdavg, epcnt, eprate


def plustoken_plot():
    source = nx.MultiDiGraph()
    # 交易哈希,区块号,时间,From,To,交易数额
    with open('./data/plustoken.csv', 'r', encoding='utf-8') as f:
        for row in csv.reader(f):
            source.add_edge(row[3], row[4], weight=float(row[5].split(' ')[0]))

    seed = '0xf4a2eff88a408ff4c4550148151c33c93442619e'
    init_g = nx.MultiDiGraph()
    init_g.add_node(seed, vis=True, dirty=290000)
    init_g.add_edges_from(source.edges(seed, data=True))

    s = OPICHaircut()
    s.push(init_g)
    for i in range(10):
        g = s.pop()
        seed = list(g.nodes)[0]
        print(seed)
        g.add_edges_from(source.edges(seed, data=True))
        s.push(g)


def tokenstore_plot():
    data = json.loads('')
    source = nx.MultiDiGraph()
    for e in data['content']['result']['edges']:
        source.add_edge(e['fm'], e['to'], weight=e['amount'])
    seed = '0x068ac6ed5efc38a6266261b4486a8907fd7ea15f'
    all_dirty = sum([e[2].get('weight', 0) for e in source.out_edges(seed, data=True)])
    init_g = nx.MultiDiGraph()
    init_g.add_node(seed, vis=True, dirty=all_dirty)
    init_g.add_edges_from(source.edges(seed, data=True))

    exs = ['ZB', 'Huobi', 'OKEx', 'Binance']

    s = OPICHaircut()
    s.push(init_g)
    opic_d = []
    for i in range(1, 30 + 1):
        g = s.pop()
        seed = list(g.nodes)[0]
        g.add_edges_from(source.edges(seed, data=True))
        s.push(g)

        dirty = 0
        for node in s.g.nodes:
            if node in exs:
                dirty += sum([e[2].get('weight') for e in s.g.in_edges(data=True)])
        opic_d.append(dirty)


def embedding():
    filenames = [
        '0xcf0e04cc0b8fcd66f42679bce42bf2569f438234',
        '0xc839ee5542b4e8413246b3634c5c739fea949562',
        '0xcceed0b7185fbe7f69c083a6c0f6ff5910548d75',
        '0xd1ceeeeee83f8bcf3bedad437202b6154e9f5405',
        '0xcf3f73290803fc04425bee135a4caeb2bab2c2a1',
        '0xd17cda470bd0237fae82ef254c84d06d0e4cc02f',
        '0x75ba02c5baf9cc3e9fe01c51df3cb1437e8690d4',
        '0xd48c88a18bfa81486862c6d1d172a39f1365e8ac',
        '0x0000871c95bb027c90089f4926fd1ba82cdd9a8b',
        '0x30008a3685f12b498d546bca7893449fa8bfb153',
        '0x000000000532b45f47779fce440748893b257865',
        '0x0000000009324b6434d7766af41908e4c49ee1d7',
        '0x00000000219ab540356cbb839cbe05303d7705fa',
        '0x00000000bf02300fd6251627aa3db8933a0eee83',
        '0x00000e32e51011e28958d4696627c82c3dacd5a6',
        '0x00067010f3ae17aa53e2b4d5142dda35380cf72d',
        '0x0020731604c882cf7bf8c444be97d17b19ea4316',
        '0x00278018530825863b765dc6cd581c0a8d471ade',
        '0x002bf459dc58584d58886169ea0e80f3ca95ffaf',
        '0x002f0c8119c16d310342d869ca8bf6ace34d9c39',
    ]

    gs = list()
    for filename in filenames:
        g = nx.Graph()
        with open('./data/Random/100_%s.csv' % filename, 'r') as f:
            for row in csv.reader(f):
                g.add_edge(row[1], row[2])

        pp = nx.Graph()
        node_map = {n: i for i, n in enumerate(g.nodes)}
        for e in g.edges:
            pp.add_edge(node_map[e[0]], node_map[e[1]])
        if pp.number_of_edges() == 0: continue
        gs.append(pp)

    from karateclub import Graph2Vec
    model = Graph2Vec()
    model.fit(gs)

    import numpy as np
    np.savetxt('./tmp', model.get_embedding())

    for filename in filenames:
        g = nx.Graph()
        with open('./data/BFS/100_%s.csv' % filename, 'r') as f:
            for row in csv.reader(f):
                g.add_edge(row[1], row[2])
        gs.append(g)

    for filename in filenames:
        g = nx.Graph()
        with open('./data/OPICHaircut/100_%s.csv' % filename, 'r') as f:
            for row in csv.reader(f):
                g.add_edge(row[1], row[2])
        gs.append(g)


def test():
    import os
    import csv
    label_map = dict()
    with open('./data/labeled_address.csv') as f:
        for row in csv.reader(f):
            label_map[row[0]] = row[1]

    random_path = './data/Random/'
    bfs_path = './data/BFS/'
    opicharicut_path = './data/OPICHaircut/'

    def statistic(path):
        nodes, edges, pnodes = set(), set(), set()
        for filename in os.listdir(path):
            with open(path + filename, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # 去掉title
                for row in reader:
                    edges.add(row[0])
                    nodes.add(row[1])
                    nodes.add(row[2])

                    if label_map.get(row[1], None):
                        pnodes.add(row[1])
                    if label_map.get(row[2], None):
                        pnodes.add(row[2])
        print(path, len(nodes), len(edges), len(pnodes), len(pnodes) / len(nodes))

    def statistic_phish(path):
        cnt = 0
        nodes, edges, pnodes, pedges = set(), set(), set(), set()
        for filename in os.listdir(path):
            if label_map.get(filename.split('_')[1].replace('.csv', '')) == 'phish-hack':
                cnt += 1
                with open(path + filename, 'r') as f:
                    reader = csv.reader(f)
                    next(reader)  # 去掉title
                    for row in reader:
                        edges.add(row[0])
                        nodes.add(row[1])
                        nodes.add(row[2])

                        if label_map.get(row[1], None):
                            pnodes.add(row[1])
                            pedges.add(row[0])
                        if label_map.get(row[2], None):
                            pnodes.add(row[2])
                            pedges.add(row[0])

        print(path, cnt, len(nodes), len(edges), len(pnodes), len(pnodes) / len(nodes), len(pedges),
              len(pedges) / len(pnodes))

    # statistic(random_path)
    # statistic(bfs_path)
    # statistic(opicharicut_path)

    statistic_phish(random_path)
    statistic_phish(bfs_path)
    statistic_phish(opicharicut_path)


if __name__ == '__main__':
    test()
    # epoch, step = 300, 10
    #
    # pcnt = [[0 for i in range(int(epoch / step))] for j in range(3)]
    # prate = [[0 for i in range(int(epoch / step))] for j in range(3)]
    # pdavg = [[0 for i in range(int(epoch / step))] for j in range(3)]
    # epcnt = [[0 for i in range(int(epoch / step))] for j in range(3)]
    # eprate = [[0 for i in range(int(epoch / step))] for j in range(3)]
    #
    # filenames = []
    # for filename in os.listdir('./data/Random'):
    #     filenames.append(filename)
    #
    # # Random
    # for i, filename in enumerate(filenames):
    #     seed = filename.split('_')[0]
    #     source_g = init_source_graph(filename)
    #     pcnt[0][i], prate[0][i], pdavg[0][i], epcnt[0][i], eprate[0][i] = \
    #         gen_phishing_plot_data(seed, Random, epoch, step, source_g)
    #
    # # BFS
    # for i, filename in enumerate(filenames):
    #     seed = filename.split('_')[0]
    #     source_g = init_source_graph(filename)
    #     pcnt[1][i], prate[1][i], pdavg[1][i], epcnt[1][i], eprate[1][i] = \
    #         gen_phishing_plot_data(seed, Random, epoch, step, source_g)
    #
    # # OPICHaircut
    # for i, filename in enumerate(filenames):
    #     seed = filename.split('_')[0]
    #     source_g = init_source_graph(filename)
    #     pcnt[2][i], prate[2][i], pdavg[2][i], epcnt[2][i], eprate[2][i] = \
    #         gen_phishing_plot_data(seed, Random, epoch, step, source_g)
