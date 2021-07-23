import json
import logging

import scrapy

from .base_txs_spider import BaseTxsSpiderSpider
from ..items import TxsItem
from ..strategies.BFS import BFS


class BFSTxsSpider(BaseTxsSpiderSpider):
    name = 'bfs_txs_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.depth = int(kwargs.get('depth', 1))
        self.seed_map = dict()

    def start_requests(self):
        seeds = self.load_seeds()
        for seed in seeds:
            self.seed_map[seed] = {
                'strategy': BFS(seed),
            }

            for tx_type in self.tx_types:
                txs = self.load_tx_cached(tx_type, seed)
                if txs is not None:
                    yield from self.parse(
                        {'result': txs},
                        **{
                            'seed': seed,
                            'address': seed,
                            'depth': 1,
                            'tx_type': tx_type,
                        }
                    )
                else:
                    yield from self.gen_tx_req(
                        tx_type=tx_type,
                        address=seed,
                        start_block=0,
                        cb_kwargs={
                            'seed': seed,
                            'address': seed,
                            'depth': 1,
                            'tx_type': tx_type,
                        },
                    )

    def parse(self, response, **kwargs):
        # loading data from response
        if isinstance(response, scrapy.http.Response):
            data = json.loads(response.text)
            if data['status'] == 0:
                logging.warning("On parse: Get error status from:%s, message:%s" % (response.url, data['message']))
                return
            logging.info('On parse: Extend {} from seed of {}, tx type is {}, depth {}'.format(
                kwargs['address'], kwargs['seed'], kwargs['tx_type'], kwargs['depth']
            ))

            if data['result'] is not None:
                # save txs
                yield TxsItem(
                    tx_type=kwargs['tx_type'],
                    address=kwargs['address'],
                    txs=data['result']
                )

                # generate next page req
                if len(data['result']) >= 10000:
                    end_tx = data['result'][-1]
                    yield from self.gen_tx_req(
                        tx_type=kwargs['tx_type'],
                        address=kwargs['address'],
                        start_block=int(end_tx['blockNumber']),
                        cb_kwargs=kwargs,
                    )
        # loading data from cache
        else:
            logging.info('On parse: Extend {} from seed of {} from cache, tx type is {}, depth {}'.format(
                kwargs['address'], kwargs['seed'], kwargs['tx_type'], kwargs['depth']
            ))
            data = response

        # detect satisfy end condition or not
        if kwargs['depth'] >= self.depth:
            return

        # push data to strategy
        if data['result'] is not None:
            self.seed_map[kwargs['seed']]['strategy'].push(data['result'])

        # next address request
        address = self.seed_map[kwargs['seed']]['strategy'].pop()
        while address is not None:
            for tx_type in self.tx_types:
                txs = self.load_tx_cached(tx_type, address)
                if txs is not None:
                    yield from self.parse(
                        {'result': txs},
                        **{
                            'seed': kwargs['seed'],
                            'address': address,
                            'depth': kwargs['depth'] + 1,
                            'tx_type': tx_type,
                        }
                    )
                else:
                    yield from self.gen_tx_req(
                        tx_type=tx_type,
                        address=address,
                        start_block=0,
                        cb_kwargs={
                            'seed': kwargs['seed'],
                            'address': address,
                            'depth': kwargs['depth'] + 1,
                            'tx_type': tx_type,
                        }
                    )
            address = self.seed_map[kwargs['seed']]['strategy'].pop()
