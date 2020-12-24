import csv
import json
import logging

import scrapy
import networkx as nx

from etherscan_spider.items import TxItem
from etherscan_spider.settings import CONCURRENT_REQUESTS
from etherscan_spider.strategies import Random, BFS, OPICHaircut
from etherscan_spider.utils import TokenBucket

APITOKENS = [
    'SQK66V2BNCHM85JJDGBP7EV4VHVTW7ZKDJ',
    # '4UX2YETIKG27YIP81XDI4SEII8DW538QU3',
    # 'XFFYAR7DBFMZB29VTQ6GR51XF2DB887X58',

    'J9996KUX8WNA5I86WY67ZMZK72SST1BIW8',
    # 'YEZRSSP7JJW93WNZ8AIM4CFEIQ1XDI8CDW',
    # 'PFPRS98QBSNWCWFG1QSBSNTDWSWD8TYT6Y',

    '4VCZMM3P2GD73WYEBC434YNTQC5R2K1EP5',
    # '8Y7KSGX5BP6DMQT8ITJPFY6DCHQIUHST24',
    # '9V1P5HYR53Q41CK6DAJTAU2UJ7IB8F8WWE',

    'JKE66VUUEHBF3A182C11PGMYSH44QC89IN',
    # 'NN8E4G2ECEIZDFHWU3IN28MIQ7SUMEYPTF',

    'YB9Y2UZKHM2V9PKIGBXYRNBATZ36T5GS8T',
]


class TxSpiderSpider(scrapy.Spider):
    name = 'tx_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.address = kwargs.get('address', None)
        self.filename = kwargs.get('file', None)
        self.address_list = list()  # 待扩展的种子列表
        self.strategy = kwargs.get('strategy', None)
        self.strategy_map = dict()
        self.apikey_bucket = TokenBucket(APITOKENS)
        self.epa = int(kwargs.get('epa', 100))  # 每个节点扩展次数
        self.ccreq = CONCURRENT_REQUESTS  # 并发爬取的子图数量

        self.label_map = dict()
        with open('./data/labeled_address.csv') as f:
            for row in csv.reader(f):
                self.label_map[row[0]] = row[1]

    def start_requests(self):
        if (self.address is None and self.filename is None) or self.apikey_bucket is None or self.strategy is None:
            self.crawler.engine.close_spider(self, 'lost arguments')

        if self.filename is not None:
            with open(self.filename, 'r') as f:
                for row in csv.reader(f):
                    self.address_list.append(row[0])

            for i in range(min(self.ccreq, len(self.address_list))):
                address = self.address_list.pop()
                yield from self.gen_start_req(address)
        elif self.address is not None:
            yield from self.gen_start_req(self.address)

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        if data['status'] == 0:
            logging.warning("On parse: Get error status from:%s" % response.url)
            return
        logging.info('On parse: Extend node index of %s from %s' % (kwargs['extend_cnt'], kwargs['address']))

        # end of crawl
        if kwargs['extend_cnt'] >= self.epa:
            yield from self.gen_items(kwargs['address'])
            del self.strategy_map[kwargs['address']]
            if len(self.address_list) > 0:
                yield from self.gen_start_req(self.address_list.pop())
            return

        # add data for g
        g = kwargs['g']
        if data['result'] is not None:
            for row in data['result']:
                g.add_edge(row['from'], row['to'], weight=int(row['value']), raw_data=row)

        # more request
        if data['result'] is None or len(data['result']) < 10000:

            # OPICHaircut add dirty value for init
            if kwargs['extend_cnt'] == 1 and isinstance(g, OPICHaircut):
                for node in g.nodes:
                    if g.nodes[node].get('vis', False):
                        g.nodes[node]['dirty'] = sum([e[2].get('weight', 0) for e in g.in_edges(node, data=True)])
                        break

            # gen next step
            g = self.gen_next_step(kwargs['address'], g)
            if g is None:
                yield from self.gen_items(kwargs['address'])
                del self.strategy_map[kwargs['address']]
                if len(self.address_list) > 0:
                    yield from self.gen_start_req(self.address_list.pop())
                return

            yield scrapy.Request(
                url=self.gen_url(self.apikey_bucket.pop(), list(g.nodes)[0], 1),
                method='GET',
                callback=self.parse,
                cb_kwargs={'address': kwargs['address'], 'g': g, 'page': 1, 'extend_cnt': kwargs['extend_cnt'] + 1},
                dont_filter=True,
            )
        else:
            # next page
            yield scrapy.Request(
                url=self.gen_url(self.apikey_bucket.pop(), kwargs['address'], kwargs['page'] + 1),
                method='GET',
                callback=self.parse,
                cb_kwargs={'address': kwargs['address'], 'g': g, 'page': kwargs['page'] + 1,
                           'extend_cnt': kwargs['extend_cnt']},
                dont_filter=True,
            )

    def gen_url(self, apikey: str, address: str, page: int):
        return 'http://api.etherscan.io/api?module=account&action=txlist' \
               '&address=%s' \
               '&offset=10000' \
               '&page=%d' \
               '&apikey=%s' % (address, page, apikey)

    def gen_start_req(self, address: str):
        Strategy = None
        if self.strategy == 'Random':
            Strategy = Random
        elif self.strategy == 'BFS':
            Strategy = BFS
        elif self.strategy == 'OPICHaircut':
            Strategy = OPICHaircut
        else:
            self.crawler.engine.close_spider(self, 'error argument of strategy')

        self.strategy_map[address] = Strategy()
        init_g = nx.MultiDiGraph()
        init_g.add_node(address, vis=True)
        yield scrapy.Request(
            url=self.gen_url(self.apikey_bucket.pop(), address, 1),
            method='GET',
            callback=self.parse,
            cb_kwargs={'address': address, 'g': init_g, 'page': 1, 'extend_cnt': 1},
            dont_filter=True,
        )

    def gen_items(self, address: str):
        logging.info('On gen_items: gen of %s' % address)
        for e in self.strategy_map[address].g.edges(data=True):
            yield TxItem(address=address, raw_data=e[2].get('raw_data'))

    def gen_next_step(self, address: str, g: nx.MultiDiGraph):
        self.strategy_map[address].push(g)
        g = self.strategy_map[address].pop()
        while g is not None and self.label_map.get(list(g.nodes)[0]) == 'exchange':
            g = self.strategy_map[address].pop()
        return g
