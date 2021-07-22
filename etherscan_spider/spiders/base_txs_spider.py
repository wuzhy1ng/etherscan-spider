import csv
import os

import scrapy

from etherscan_spider.settings import APITOKENS
from etherscan_spider.utils import TokenBucket


class BaseTxsSpiderSpider(scrapy.Spider):
    name = 'base_txs_spider'
    allowed_domains = ['*']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # get seeds from arguments or file
        self.seed = kwargs.get('seed')
        self.seeds_fn = kwargs.get('seeds_fn')
        assert self.seed or self.seeds_fn, "'seed' or 'seeds_fn' arguments are needed"
        self.seeds = set()

        # get cache dir
        self.cache_dir = kwargs.get('cache_dir', './data/cache')

        # get output dir
        self.output_dir = kwargs.get('output_dir', './data/%s' % self.name)

        # get tx types
        self.tx_types = kwargs.get('tx_types', None)
        self.tx_types_allowed = {
            'external': 'txlist',
            'internal': 'txlistinternal',
            'erc20': 'tokentx',
            'erc721': 'tokennfttx',
        }
        if self.tx_types is None:
            self.tx_types = set(self.tx_types_allowed.values())
        else:
            tx_types = set()
            for tx_type in set(self.tx_types.split(',')):
                if self.tx_types_allowed.get(tx_type) is not None:
                    tx_types.add(self.tx_types_allowed[tx_type])
            self.tx_types = tx_types

        # init token bucket
        self.apikey_bucket = TokenBucket(APITOKENS)

    def _load_crawled_seeds(self) -> set:
        crawled_fn = os.path.join(self.cache_dir, 'crawled.csv')
        crawled_seeds = set()
        if not os.path.exists(crawled_fn):
            with open(crawled_fn, 'w', newline='') as f:
                csv.writer(f).writerow(['address'])
        with open(crawled_fn, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                crawled_seeds.add(row[0])
        return crawled_seeds

    def _load_seeds(self, crawled_seeds: set = None) -> set:
        seeds = set()
        if self.seeds_fn is not None and os.path.exists(self.seeds_fn):
            with open(self.seeds_fn, 'r') as f:
                for row in csv.reader(f):
                    if crawled_seeds is not None and row[0] in crawled_seeds:
                        continue
                    seeds.add(row[0])
        elif self.seed is not None:
            seeds.add(self.seed)
        return seeds

    def start_requests(self):
        raise NotImplementedError()

    def parse(self, response, **kwargs):
        raise NotImplementedError()

    def _gen_tx_req(
            self,
            action: str,
            address: str,
            start_block: int = 0,
            req_params: dict = None,
            cb_kwargs: dict = None
    ):
        url = 'http://api.etherscan.io/api?module=account&action=%s' \
              '&address=%s' \
              '&offset=10000' \
              '&startblock=%d' \
              '&apikey=%s' % (action, address, start_block, self.apikey_bucket.pop())
        if req_params is not None:
            for k, v in req_params.items():
                url += '&{}={}'.format(k, v)

        yield scrapy.Request(
            url=url,
            method='GET',
            callback=self.parse,
            cb_kwargs=cb_kwargs if cb_kwargs else dict(),
            dont_filter=True,
        )

    def _gen_tx_reqs(
            self,
            address: str,
            start_block: int = 0,
            req_params: dict = None,
            cb_kwargs: dict = None
    ):
        for tx_type in self.tx_types:
            yield self._gen_tx_req(
                action=tx_type,
                address=address,
                start_block=start_block,
                req_params=req_params,
                cb_kwargs=cb_kwargs,
            )
