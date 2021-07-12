import argparse
import csv
import os

import networkx as nx


class TTRLocalCommunityExtractor:
    def __init__(self, phi: float = 1e-2):
        self.phi = phi

    def extract(self, g: nx.MultiDiGraph, seed, ttr) -> nx.MultiDiGraph:
        def _calc_conductance_incr(inter_sum, outer_sum, new_node, g, inter_nodes, outer_nodes, ttr):
            inter_nodes.add(new_node)
            inter_sum += ttr[new_node]

            if new_node in outer_nodes:
                outer_sum -= ttr.get(new_node, 0)

            for e in g.in_edges(new_node):
                if e[0] not in inter_nodes and e[0] not in outer_nodes:
                    outer_nodes.add(e[0])
                    outer_sum += ttr.get(e[0], 0)
            for e in g.out_edges(new_node):
                if e[1] not in inter_nodes and e[1] not in outer_nodes:
                    outer_nodes.add(e[1])
                    outer_sum += ttr.get(e[1], 0)
            return inter_sum, outer_sum, inter_nodes, outer_nodes

        inter_nodes = set()
        outer_nodes = set()
        inter_sum, outer_sum = 0, 0

        ttr_items = sorted([(k, v) for k, v in ttr.items()], key=lambda x: x[1])
        while True:
            new_node, weight = ttr_items.pop()
            inter_sum, outer_sum, inter_nodes, outer_nodes = _calc_conductance_incr(
                inter_sum, outer_sum, new_node, g, inter_nodes, outer_nodes, ttr
            )
            if outer_sum / (inter_sum + outer_sum) < self.phi or len(ttr_items) == 0:
                break

        return g.subgraph(inter_nodes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.description = 'generate local community by ttr'
    parser.add_argument('-i', '--input', help='input raw data folder', dest='input', type=str)
    parser.add_argument('-o', '--output', help='output data folder', dest='output', type=str)
    parser.add_argument('--addr', type=str, dest='addr', help='address of csv')
    args = parser.parse_args()

    g = nx.MultiDiGraph()
    tx_hash = set()
    in_fn = os.path.join(args.input, args.addr + '.csv')
    with open(in_fn, 'r') as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            if row[0] in tx_hash:
                continue
            tx_hash.add(row[0])
            g.add_edge(row[1], row[2], weight=row[3], hash=row[0], time_stamp=row[5])

    ttr = dict()
    in_fn = os.path.join(args.input, args.addr + '_ttr.csv')
    with open(in_fn, 'r') as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            ttr[row[0]] = float(row[1])
    g = TTRLocalCommunityExtractor().extract(g, args.addr, ttr)

    if not os.path.exists(args.output):
        os.mkdir(args.output)

    out_fn = os.path.join(args.output, args.addr + '_edges.csv')
    with open(out_fn, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Source', 'Target', 'Weight', 'TimeStamp', 'hash'])
        for s, t, e_args in g.edges(data=True):
            writer.writerow([s, t, e_args['weight'], e_args['time_stamp'], e_args['hash']])

    out_fn = os.path.join(args.output, args.addr + '_nodes.csv')
    with open(out_fn, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Id', 'Label', 'ttr'])
        for node in g.nodes():
            writer.writerow([node, node[:5], ttr[node]])
        # for k, v in ttr.items():
        #     writer.writerow([k, k[:5], v])

    # es_hash = set()
    # with open(
    #         '/Users/leicx/wuzhiying/projects/eth_spider/etherscan-spider/data/coinholmes-0xf4a2eff88a408ff4c4550148151c33c93442619e.csv',
    #         'r'
    # ) as f:
    #     reader = csv.reader(f)
    #     next(reader)
    #     for row in reader:
    #         es_hash.add(row[0])

    # ex_hash = {
    #     '0xf0d9fcb4fefdbd3e7929374b4632f8ad511bd7e3',
    #     '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',
    #     '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa',
    #     '0x034f854b44d28e26386c1bc37ff9b20c6380b00d',
    #     '0xf775a9a0ad44807bc15936df0ee68902af1a0eee',
    # }
    # ex_hash = {
    #     '0x8d12a197cb00d4747a1fe03395095ce2a5cc6819',
    #     '0xc5883084a66ac9e08379256269c18345ccefe458',
    #     '0x7d90b19c1022396b525c64ba70a293c3142979b7',
    #     '0x621e2e9f1cdc03add35ab930b074f9419f294045',
    # }
    # ex_hash = {
    #     '0xf0d9fcb4fefdbd3e7929374b4632f8ad511bd7e3',
    #     '0x167a9333bf582556f35bd4d16a7e80e191aa6476',
    #     '0x96faaeefd3dabdd10dbe928d75884ef22ec157db',
    #     '0x5e032243d507c743b061ef021e2ec7fcc6d3ab89',
    #     '0xb9a4873d8d2c22e56b8574e8605644d08e047549',
    #     '0xf775a9a0ad44807bc15936df0ee68902af1a0ee',
    #     '0xd8a83b72377476d0a66683cde20a8aad0b628713',
    #     '0x034f854b44d28e26386c1bc37ff9b20c6380b00d',
    #     '0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94',
    #     '0xb9a4873d8d2c22e56b8574e8605644d08e047549',
    #     '0x5861b8446a2f6e19a067874c133f04c578928727',
    #     '0x6f50c6bff08ec925232937b204b0ae23c488402a',
    #     '0x39d9f4640b98189540a9c0edcfa95c5e657706aa',
    #     '0xf775a9a0ad44807bc15936df0ee68902af1a0eee',
    #     '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',
    #     '0x034f854b44d28e26386c1bc37ff9b20c6380b00d',
    #     '0x0d0707963952f2fba59dd06f2b425ace40b492fe',
    # }
    #
    #
    # hit_hash = set()
    # hit_ex = set()
    # for e in g.edges(data=True):
    #     if e[2]['hash'] in es_hash:
    #         hit_hash.add(e[2]['hash'])
    #     if e[0] in ex_hash:
    #         hit_ex.add(e[0])
    #     if e[1] in ex_hash:
    #         hit_ex.add(e[1])
    # tmp = nx.single_source_shortest_path_length(g, args.addr)
    # print(min(list(tmp.values())), max(list(tmp.values())))
    # print(len(hit_hash), len(es_hash))
    # print(len(hit_ex), len(ex_hash))
    # print(hit_ex)
    # print(g.number_of_edges(), g.number_of_nodes())
