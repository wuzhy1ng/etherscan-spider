import csv
import json
import logging
import time

import scrapy

from etherscan_spider.items import TxItem, CloseItem
from etherscan_spider.settings import APITOKENS
from etherscan_spider.strategies import OPICHaircut
from etherscan_spider.utils import TokenBucket
from etherscan_spider.utils.strategy_evaluate import StrategyEvaluator


class OpichaircutTxSpiderSpider(scrapy.Spider):
    name = 'opichaircut_tx_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 输入种子
        self.seed = kwargs.get('seed', None)
        self.filename = kwargs.get('file', None)
        assert self.seed or self.filename, "'seed' or 'file' arguments are needed"

        self.except_filename = kwargs.get('file_expect', None)  # 无需爬取的种子文件
        self.out_path = kwargs.get('out', './data/OPICHaircut')  # 输出文件夹路径

        # 字段屏蔽
        self.field_mask = kwargs.get('mask', '')
        self.field_mask = set(self.field_mask.split(','))

        # 爬虫终止条件
        self.epa = kwargs.get('epa', None)  # 每个节点扩展次数

        # 保证爬虫终止条件只有一个
        conds = [('epa', self.epa)]
        conds_bool = [1 if value is not None else 0 for _, value in conds]
        assert sum(conds_bool) == 1, \
            'one end condition needed refer to ' + ','.join(["'%s'" % cond_name for cond_name, _ in conds])
        self.cond_name, self.cond_value = conds[conds_bool.index(1)][0], int(conds[conds_bool.index(1)][1])

        self.seed_list = list()  # 待扩展的种子列表
        self.seed_map = dict()  # 与种子爬取任务相关信息
        self.strategy = OPICHaircut  # 爬虫策略
        self.apikey_bucket = TokenBucket(APITOKENS)  # 控制并发请求数量的令牌桶

        # 策略效果可视化组件
        self.has_evaluator = kwargs.get('evaluate', False)
        if self.has_evaluator:
            observer_labels = [label for label in kwargs.get('labels').split(',')]
            self.evaluator = StrategyEvaluator(
                observer_labels=observer_labels,
                log_name='OPICHaircut_' + str(time.time()),
            )

        self.label_map = dict()
        with open('./data/labeled_address.csv') as f:
            for row in csv.reader(f):
                self.label_map[row[0]] = row[1]

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
                    self.seed_map[row[0]] = {'strategy': self.strategy(row[0]), self.cond_name: 1}
        # 以参数形式输入种子
        elif self.seed is not None:
            self.seed_list.append(self.seed)
            self.seed_map[self.seed] = {'strategy': self.strategy(self.seed), self.cond_name: 1}

        # 发出请求
        for seed in self.seed_list:
            yield from self.gen_req(seed, seed, 1, **{self.cond_name: 1})

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        if data['status'] == 0:
            logging.warning("On parse: Get error status from:%s" % response.url)
            return
        logging.info(
            'On parse: Extend {} from seed of {}, control var {}: {}'.format(
                kwargs['address'], kwargs['seed'],
                self.cond_name, kwargs[self.cond_name],
            )
        )

        # process tx
        if data['result'] is not None:
            # save tx
            for row in data['result']:
                yield TxItem(seed=kwargs['seed'], tx=row)

            # save evaluator state
            if self.has_evaluator:
                self.evaluator.update_state(data['result'])

        # detect satisfy end condition or not
        if self.satisfy_ending_cond(kwargs['seed'], **{self.cond_name: kwargs[self.cond_name]}):
            self.seed_map[kwargs['seed']] = {self.cond_name: self.seed_map[kwargs['seed']][self.cond_name]}
            logging.info("On parse: %s finished" % kwargs['seed'])
            yield CloseItem(seed=kwargs['seed'])
            return

        # push data to strategy
        if data['result'] is not None:
            for i in range(len(data['result'])):
                data['result'][i]['value'] = float(data['result'][i]['value'])
            self.seed_map[kwargs['seed']]['strategy'].push(kwargs['address'], data['result'])

        # next address request
        if data['result'] is None or len(data['result']) < 10000:
            if self.has_next(kwargs['seed']):
                address = self.seed_map[kwargs['seed']]['strategy'].pop()
                yield from self.gen_req(kwargs['seed'], address, 1, **{self.cond_name: kwargs[self.cond_name] + 1})
        # next page request
        else:
            yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1,
                                    **{self.cond_name: kwargs[self.cond_name]})

    def satisfy_ending_cond(self, seed: str, **kwargs):
        cond_value = self.seed_map[seed][self.cond_name]
        if self.cond_name == 'epa':
            if cond_value >= self.cond_value:
                return True
            return False

    def has_next(self, seed: str):
        cond_value = self.seed_map[seed][self.cond_name]
        if self.cond_name == 'epa':
            if cond_value >= self.cond_value:
                return False
            self.seed_map[seed][self.cond_name] += 1
            return True

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
                cb_kwargs={'seed': seed, 'address': address, 'page': page, self.cond_name: kwargs.get(self.cond_name)},
                dont_filter=True,
                priority=kwargs.get(self.cond_name)
            )

    def req_filter(self, address: str):
        if address is None \
                or len(address) < 42:
            return None
        return address
