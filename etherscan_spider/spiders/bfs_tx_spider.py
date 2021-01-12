import csv
import json
import logging

import scrapy

from etherscan_spider.items import TxItem, CloseItem
from etherscan_spider.settings import APITOKENS
from etherscan_spider.strategies import BFS
from etherscan_spider.utils import TokenBucket


class BfsTxSpiderSpider(scrapy.Spider):
    name = 'bfs_tx_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.seed = kwargs.get('seed', None)
        self.filename = kwargs.get('file', None)
        self.epa = int(kwargs.get('epa', 100))  # 每个节点扩展次数
        self.depth = kwargs.get('depth', None)  # 爬取深度
        if self.depth:
            self.depth = int(self.depth)

        self.seed_list = list()  # 待扩展的种子列表
        self.seed_map = dict()  # 与种子爬取任务相关信息
        self.strategy = BFS  # 爬虫策略
        self.apikey_bucket = TokenBucket(APITOKENS)

        self.label_map = dict()
        with open('./data/labeled_address.csv') as f:
            for row in csv.reader(f):
                self.label_map[row[0]] = row[1]

    def start_requests(self):
        if self.seed is None and self.filename is None:
            self.crawler.engine.close_spider(self, 'lost arguments')

        if self.filename is not None:
            with open(self.filename, 'r') as f:
                for row in csv.reader(f):
                    self.seed_list.append(row[0])
                    self.seed_map[row[0]] = {'strategy': self.strategy(row[0]), 'extend_count': 1}
        elif self.seed is not None:
            self.seed_list.append(self.seed)
            self.seed_map[self.seed] = {'strategy': self.strategy(self.seed), 'extend_count': 1}

        for seed in self.seed_list:
            yield from self.gen_req(seed, seed, 1, 1, 1)

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        if data['status'] == 0:
            logging.warning("On parse: Get error status from:%s" % response.url)
            # TODO: 错误处理
            return
        logging.info('On parse: Extend node index of %s from %s' % (kwargs['extend_count'], kwargs['address']))

        # save tx
        if data['result'] is not None:
            for row in data['result']:
                yield TxItem(seed=kwargs['seed'], tx=row)

        # if strategy need to extend
        seed = kwargs['seed']
        if self.seed_map.get(seed, None) is not None:
            # push data to strategy
            if data['result'] is not None:
                self.seed_map[seed]['strategy'].push(data['result'])

            # next address request
            if data['result'] is None or len(data['result']) < 10000:
                while True:
                    # 以深度作为爬取结束条件
                    # 以扩展次数作为爬取结束条件
                    if (self.depth is not None and kwargs['depth'] >= self.depth) or \
                            self.seed_map[seed]['extend_count'] + 1 > self.epa:
                        del self.seed_map[seed]
                        logging.info("On parse: %s finished" % seed)
                        yield CloseItem(seed=seed)
                        break

                    address = self.seed_map[seed]['strategy'].pop()
                    if address is None:
                        break
                    self.seed_map[seed]['extend_count'] += 1
                    yield from self.gen_req(seed, address, 1, self.seed_map[seed]['extend_count'], kwargs['depth'] + 1)
            # next page request
            else:
                yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1, kwargs['extend_count'],
                                        kwargs['depth'])

    def gen_url(self, apikey: str, address: str, page: int):
        return 'http://api.etherscan.io/api?module=account&action=txlist' \
               '&address=%s' \
               '&offset=10000' \
               '&page=%d' \
               '&apikey=%s' % (address, page, apikey)

    def gen_req(self, seed: str, address: str, page: int, extend_count: int, depth: int):
        if self.req_filter(address) is not None:
            yield scrapy.Request(
                url=self.gen_url(self.apikey_bucket.pop(), address, page),
                method='GET',
                callback=self.parse,
                cb_kwargs={'seed': seed, 'address': address, 'page': page, 'extend_count': extend_count,
                           'depth': depth},
                dont_filter=True,
                priority=extend_count,
            )

    def req_filter(self, address: str):
        if address is None \
                or (self.label_map.get(address) and self.label_map[address] == 'exchange') \
                or len(address) < 42:
            return None
        return address
