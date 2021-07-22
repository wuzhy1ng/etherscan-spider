import csv
import json
import logging
import os
import time

import scrapy

from etherscan_spider.items import TxItem, TTRItem, FirstOrderNetItem
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
        self.cache_path = os.path.join(
            kwargs.get('cache_dir', './data/TTR/tmp'),
            str(int(time.time()))
        )  # 一阶子图缓存路径

        # 字段屏蔽
        self.field_mask = kwargs.get('mask', '')
        self.field_mask = set(self.field_mask.split(','))

        # 策略参数
        self.alpha = float(kwargs.get('alpha', 0.15))
        self.beta = float(kwargs.get('beta', 0.8))
        self.epsilon = float(kwargs.get('epsilon', 1e-5))

        self.seed_list = list()  # 待扩展的种子列表
        self.seed_map = dict()  # 与种子爬取任务相关信息
        self.strategy = TTR  # 爬虫策略
        self.apikey_bucket = TokenBucket(APITOKENS)  # 控制并发请求数量的令牌桶

        # 起始请求控制参数
        # self.start_args = eval(kwargs.get('start_args', '{}'))
        self.start_block = kwargs.get('startblock', None)
        self.end_block = kwargs.get('endblock', None)

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
                    self.seed_map[row[0]] = {'strategy': self.strategy(row[0], self.alpha, self.beta, self.epsilon)}

        # 以参数形式输入种子
        elif self.seed is not None:
            self.seed_list.append(self.seed)
            self.seed_map[self.seed] = {'strategy': self.strategy(self.seed, self.alpha, self.beta, self.epsilon)}

        # 发出请求
        for seed in self.seed_list:
            yield from self.gen_req(
                seed, seed, 1,
                **{'startblock': self.start_block, 'endblock': self.end_block}
            )

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

        txs = kwargs['cached_txs'] if kwargs['cached_txs'] is not None else list()
        if data['result'] is not None:
            # format data type
            for i in range(len(data['result'])):
                data['result'][i]['timeStamp'] = int(data['result'][i]['timeStamp'])
                data['result'][i]['value'] = float(data['result'][i]['value'])
                txs.append(data['result'][i])

            # next page
            if len(data['result']) >= 10000:
                yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1, txs)
                return

        # save first order net
        yield FirstOrderNetItem(
            seed=kwargs['address'],
            txs=txs,
        )

        # kernel process
        strategy = self.seed_map[kwargs['seed']]['strategy']
        address = kwargs['address']
        while True:
            # push data to strategy and save tx
            for tx in strategy.push(address, txs):
                yield TxItem(seed=strategy.source, tx=tx)

            # generate next address or stop strategy
            address = strategy.pop()
            if address is None:
                yield TTRItem(
                    seed=strategy.source,
                    p=strategy.p,
                )
                break

            # load cached first order net or generate request
            txs = self.load_cached_first_order_net(address)
            if txs is None:
                yield from self.gen_req(strategy.source, address, 1)
                break
            logging.info('On parse: Using cached net with seed of ' + address)

        # # process tx
        # if data['result'] is not None:
        #     # format data type
        #     for i in range(len(data['result'])):
        #         data['result'][i]['timeStamp'] = int(data['result'][i]['timeStamp'])
        #         data['result'][i]['value'] = float(data['result'][i]['value'])
        #
        #     # push data to strategy and save tx
        #     for tx in self.seed_map[kwargs['seed']]['strategy'].push(kwargs['address'], data['result']):
        #         yield TxItem(seed=kwargs['seed'], tx=tx)
        #
        # # next address request
        # if data['result'] is None or len(data['result']) < 10000:
        #     address = self.seed_map[kwargs['seed']]['strategy'].pop()
        #     if address is not None:
        #         yield from self.gen_req(kwargs['seed'], address, 1)
        #     else:
        #         yield TTRItem(
        #             seed=kwargs['seed'],
        #             p=self.seed_map[kwargs['seed']]['strategy'].p,
        #         )
        #
        # # next page request
        # else:
        #     yield from self.gen_req(kwargs['seed'], kwargs['address'], kwargs['page'] + 1)

    def gen_req(self, seed: str, address: str, page: int, cached_txs: list = None, **kwargs):
        url = 'http://api.etherscan.io/api?module=account&action=txlist' \
              '&address=%s' \
              '&offset=10000' \
              '&page=%d' \
              '&apikey=%s' % (address, page, self.apikey_bucket.pop())
        if kwargs is not None:
            for k, v in kwargs.items():
                url += '&{}={}'.format(k, v)

        if self.req_filter(address) is not None:
            yield scrapy.Request(
                url=url,
                method='GET',
                callback=self.parse,
                cb_kwargs={'seed': seed, 'address': address, 'page': page, 'cached_txs': cached_txs},
                dont_filter=True,
            )

    def req_filter(self, address: str):
        if address is None \
                or len(address) < 42:
            return None
        return address

    def kernel_process(self, strategy: TTR, address, data: list):
        """
        1. 将数据推入策略中，生成命中的tx
        2. 查询下一个需要扩展的地址，如果没有就返回
        3. 否则查询该地址是否有缓存的一阶子图，如果没有就返回请求
        4. 否则读取缓存的一阶子图推入策略
        :param strategy:
        :param address:
        :param data:
        :return:
        """

        while True:
            for tx in strategy.push(address, data):
                yield TxItem(seed=strategy.source, tx=tx)

            address = strategy.pop()
            if address is None:
                yield None
                break

            data = self.load_cached_first_order_net(address)
            if data is None:
                yield self.gen_req(strategy.source, address, 1)
                break

    def load_cached_first_order_net(self, address: str) -> list:
        fn = os.path.join(self.cache_path, address + '.csv')
        if os.path.exists(fn):
            txs = list()
            fields = [
                'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed',
                'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
            ]
            with open(fn, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    tx = {fields[i]: row[i] for i in range(len(fields))}
                    tx['value'] = float(tx['value'])
                    tx['timeStamp'] = int(tx['timeStamp'])
                    txs.append(tx)
            return txs
        return None
