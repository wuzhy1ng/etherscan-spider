import csv
import json
import logging

import scrapy

from etherscan_spider.items import TxItem
from etherscan_spider.settings import APITOKENS
from etherscan_spider.strategies.TTR import TTR
from etherscan_spider.utils import TokenBucket


class TTRTxSpider(scrapy.Spider):
    name = 'ttr_tx_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 输入种子
        self.seed = kwargs.get('seed', None)
        self.filename = kwargs.get('file', None)
        assert self.seed or self.filename, "'seed' or 'file' arguments are needed"

        self.except_filename = kwargs.get('file_expect', None)  # 无需爬取的种子文件
        self.out_path = kwargs.get('out', './data/TTR')  # 输出文件夹路径

        # 字段屏蔽
        self.field_mask = kwargs.get('mask', '')
        self.field_mask = set(self.field_mask.split(','))

        # 策略参数
        self.alpha = kwargs.get('alpha', 0.15)
        self.beta = kwargs.get('beta', 0.8)
        self.epsilon = kwargs.get('epsilon', 1e-5)

        self.seed_list = list()  # 待扩展的种子列表
        self.seed_map = dict()  # 与种子爬取任务相关信息
        self.strategy = TTR  # 爬虫策略
        self.apikey_bucket = TokenBucket(APITOKENS)  # 控制并发请求数量的令牌桶

    def start_requests(self):
        # 读取无需爬取的种子
        except_seed = None
        if self.except_filename is not None:
            except_seed = set()
            with open(self.except_filename, 'r') as f:
                for row in csv.reader(f):
                    except_seed.add(row[0])

        # 以文件形式输入种子
        if self.filename is not None:
            with open(self.filename, 'r') as f:
                for row in csv.reader(f):
                    if except_seed is not None and row[0] in except_seed:
                        continue
                    self.seed_list.append(row[0])
                    self.seed_map[row[0]] = {'strategy': self.strategy(row[0])}
        # 以参数形式输入种子
        elif self.seed is not None:
            self.seed_list.append(self.seed)
            self.seed_map[self.seed] = {'strategy': self.strategy(self.seed)}

        # 发出请求
        for seed in self.seed_list:
            yield from self.gen_req(seed, seed, 1)

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        if data['status'] == 0:
            logging.warning("On parse: Get error status from:%s" % response.url)
            return
        logging.info(
            'On parse: Extend {} from seed of {}'.format(
                kwargs['address'], kwargs['seed'],
            )
        )

        # process tx
        if data['result'] is not None:
            # save tx
            for row in data['result']:
                yield TxItem(seed=kwargs['seed'], tx=row)

        # push data to strategy
        if data['result'] is not None:
            for i in range(len(data['result'])):
                data['result'][i]['value'] = float(data['result'][i]['value'])
            self.seed_map[kwargs['seed']]['strategy'].push(kwargs['address'], data['result'])

        # next address request
        if data['result'] is None or len(data['result']) < 10000:
            address = self.seed_map[kwargs['seed']]['strategy'].pop()
            if address is not None:
                yield from self.gen_req(kwargs['seed'], address, 1)
        # next page request
        else:
            yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1)

    def gen_req(self, seed: str, address: str, page: int, **kwargs):
        url = 'http://api.etherscan.io/api?module=account&action=txlist' \
              '&address=%s' \
              '&offset=10000' \
              '&page=%d' \
              '&apikey=%s' % (address, page, self.apikey_bucket.pop())

        if self.req_filter(address) is not None:
            yield scrapy.Request(
                url=url,
                method='GET',
                callback=self.parse,
                cb_kwargs={'seed': seed, 'address': address, 'page': page},
                dont_filter=True,
            )

    def req_filter(self, address: str):
        if address is None \
                or len(address) < 42:
            return None
        return address
