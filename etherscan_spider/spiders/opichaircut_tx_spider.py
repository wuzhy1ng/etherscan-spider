import csv
import json
import logging

import scrapy

from etherscan_spider.items import TxItem, CloseItem
from etherscan_spider.settings import APITOKENS
from etherscan_spider.strategies import OPICHaircut
from etherscan_spider.utils import TokenBucket


class OpichaircutTxSpiderSpider(scrapy.Spider):
    name = 'opichaircut_tx_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.seed = kwargs.get('seed', None)
        self.filename = kwargs.get('file', None)
        self.except_filename = kwargs.get('file_expect', None)  # 无需爬取的种子文件
        self.epa = int(kwargs.get('epa', 100))  # 每个节点扩展次数

        self.seed_list = list()  # 待扩展的种子列表
        self.seed_map = dict()  # 与种子爬取任务相关信息
        self.strategy = OPICHaircut  # 爬虫策略
        self.all_dirty = 66
        self.apikey_bucket = TokenBucket(APITOKENS)

        self.label_map = dict()
        with open('./data/labeled_address.csv') as f:
            for row in csv.reader(f):
                self.label_map[row[0]] = row[1]

    def start_requests(self):
        if self.seed is None and self.filename is None:
            self.crawler.engine.close_spider(self, 'lost arguments')

        # 读取无需爬取的种子
        except_seed = None
        if self.except_filename is not None:
            except_seed = set()
            with open(self.except_filename, 'r') as f:
                for row in csv.reader(f):
                    except_seed.add(row[0])

        # 读取种子文件
        if self.filename is not None:
            with open(self.filename, 'r') as f:
                for row in csv.reader(f):
                    if except_seed is not None and row[0] in except_seed:
                        continue
                    self.seed_list.append(row[0])
                    self.seed_map[row[0]] = {'strategy': self.strategy(row[0], self.all_dirty), 'extend_count': 1}
        elif self.seed is not None:
            self.seed_list.append(self.seed)
            self.seed_map[self.seed] = {'strategy': self.strategy(self.seed, self.all_dirty), 'extend_count': 1}

        for seed in self.seed_list:
            yield from self.gen_req(seed, seed, 1, 1)

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
                for i in range(len(data['result'])):
                    data['result'][i]['value'] = float(data['result'][i]['value'])
                self.seed_map[seed]['strategy'].push(kwargs['address'], data['result'])

            # next address request
            if data['result'] is None or len(data['result']) < 10000:
                if self.seed_map[seed]['extend_count'] + 1 < self.epa:
                    address = self.seed_map[seed]['strategy'].pop()
                    self.seed_map[seed]['extend_count'] += 1
                    yield from self.gen_req(seed, address, 1, self.seed_map[seed]['extend_count'])
                else:
                    del self.seed_map[seed]
                    logging.info("On parse: %s finished" % seed)
                    yield CloseItem(seed=seed)
            # next page request
            else:
                yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1, kwargs['extend_count'])

    def gen_url(self, apikey: str, address: str, page: int):
        return 'http://api.etherscan.io/api?module=account&action=txlist' \
               '&address=%s' \
               '&offset=10000' \
               '&page=%d' \
               '&apikey=%s' % (address, page, apikey)

    def gen_req(self, seed: str, address: str, page: int, extend_count: int):
        if self.req_filter(address) is not None:
            yield scrapy.Request(
                url=self.gen_url(self.apikey_bucket.pop(), address, page),
                method='GET',
                callback=self.parse,
                cb_kwargs={'seed': seed, 'address': address, 'page': page, 'extend_count': extend_count},
                dont_filter=True,
                priority=extend_count,
            )

    def req_filter(self, address: str):
        if address is None \
                or len(address) < 42:
            return None
        return address
