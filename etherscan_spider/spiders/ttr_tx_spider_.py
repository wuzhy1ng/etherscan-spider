import csv
import json
import logging
import time

import scrapy

from etherscan_spider.spiders.base_tx_spider import BaseTxSpider
from etherscan_spider.strategies import TTR, StrategyFactory


class TTRTxSpider(BaseTxSpider):
    name = 'ttr_tx_spider_'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(TTR, **kwargs)

        # exchange rate base on eth
        self.exchange_rate = dict()
        self.exchange_rate_fn = kwargs.get('exchange_rate_fn')

    def load_exchange_rate(self):
        with open(self.exchange_rate_fn, 'r') as f:
            data = json.loads(f.read())


    def start_requests(self):
        # load query params from kwargs, whose key has prefix of '_'
        query_params = self.load_query_params()

        # create strategy instances for seeds and generate requests
        for seed in self.load_seeds():
            self.seed_map[seed] = {
                'strategy': StrategyFactory(**self.kwargs).create_strategy(self.strategy),
                'cached_txs': dict(),
            }

            # generate normal tx request
            yield from self.gen_req(
                action='txlist',
                seed=seed,
                address=seed,
                page=1,
                **query_params
            )
            self.seed_map[seed]['cached_txs']['_'.join(['txlist', seed, str(1)])] = None

            # generate ERC20 tx request
            yield from self.gen_req(
                action='tokentx',
                seed=seed,
                address=seed,
                page=1,
                **query_params
            )
            self.seed_map[seed]['cached_txs']['_'.join(['tokentx', seed, str(1)])] = None

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        if data['status'] == 0:
            logging.warning("On parse: Get error status from:%s" % response.url)
            return
        logging.info('On parse: Fetched {} from seed of {} with action \'{}\''.format(
            kwargs['address'], kwargs['seed'], kwargs['action'],
        ))

        # read tx from response
        txs = list()
        cached_txs = self.seed_map[kwargs['seed']]['cached_txs']
        if data['result'] is not None:
            # format data type
            for i in range(len(data['result'])):
                tx = data['result'][i]
                tx['timeStamp'] = int(tx['timeStamp'])
                tx['value'] = float(tx['value'])
                txs.append(tx)

            # next page
            if len(data['result']) >= 10000:
                yield from self.gen_req(
                    action=kwargs['action'],
                    seed=kwargs['seed'],
                    address=kwargs['address'],
                    page=kwargs['page'] + 1,
                )
                cached_txs['_'.join([kwargs['action'], kwargs['address'], kwargs['page'] + 1])] = None

        # cache txs
        cached_txs['_'.join([kwargs['action'], kwargs['address'], kwargs['page']])] = txs
        for v in cached_txs.values():
            if v is None:
                return

        # fetching exchange rate
        tokens = set()
        for k, v in cached_txs.items():
            if k.split('_')[0] == 'tokentx':
                for tx in v:
                    tokens.add(tx['tokenSymbol'])

        self.kernel_process(kwargs['address'])

    def kernel_process(self, address):
        pass
