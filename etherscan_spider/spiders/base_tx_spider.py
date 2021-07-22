import csv
import os
import time

import scrapy

from etherscan_spider.settings import APITOKENS
from etherscan_spider.utils import TokenBucket


class BaseTxSpider(scrapy.Spider):
    name = 'base_tx_spider'
    allowed_domains = ['*']

    def __init__(self, strategy, **kwargs):
        super().__init__(**kwargs)

        # input seeds
        self.seed = kwargs.get('seed', None)
        self.seeds_fn = kwargs.get('seeds_file', None)
        assert self.seed or self.seeds_fn, "'seed' or 'seeds_file' arguments are needed"

        # cache crawled seeds or filter crawled seeds
        self.crawled_output_fn = kwargs.get('crawled_output', './data/crawled.csv')
        self.crawled_input_fn = kwargs.get('crawled_input', None)

        # output path and cache path
        self.out_path = kwargs.get('out', './data/%s' % self.name)
        self.cache_path = os.path.join(
            kwargs.get('cache_dir', './data/tmp'),
            str(int(time.time()))
        )

        # field mask or field filter
        self.field_mask = kwargs.get('field_mask')
        self.field_mask = set(self.field_mask.split(',')) if self.field_mask else None
        self.field_filter = kwargs.get('field_filter')
        self.field_filter = set(self.field_filter.split(',')) if self.field_filter else None

        # strategy class, mapping between seed and strategy instance
        self.strategy = strategy
        self.seed_map = dict()

        # token bucket to control request per second
        self.apikey_bucket = TokenBucket(APITOKENS)

        # cache all arguments for extending
        self.kwargs = kwargs

    def load_seeds(self) -> set:
        """
        load seeds from file or cmdline argument, and filter crawled seeds
        :return: seeds required crawling
        """
        # load crawled seeds
        crawled_seeds = set()
        if self.crawled_input_fn is not None:
            with open(self.crawled_input_fn, 'r') as f:
                for row in csv.reader(f):
                    crawled_seeds.add(row[0])

        # load seeds from file
        seeds = set()
        if self.seeds_fn is not None:
            with open(self.seeds_fn, 'r') as f:
                for row in csv.reader(f):
                    if row[0] in crawled_seeds:
                        continue
                    seeds.add(row[0])

        # load seed from argument
        elif self.seed is not None:
            seeds.add(self.seed)

        return seeds

    def load_query_params(self):
        """
        load query params from kwargs, whose key has prefix of '_'
        :return: query params
        """
        query_params = dict()
        for k, v in self.kwargs.items():
            if k.startswith('_'):
                query_params[k[1:]] = v
        return query_params

    def start_requests(self):
        raise NotImplementedError()

    def parse(self, response, **kwargs):
        raise NotImplementedError()

    def gen_req(self, action: str, seed: str, address: str, page: int, **kwargs):
        """
        generate normal tx request or ERC20 tx request
        :param action: 'txlist' for normal tx, or 'tokentx' for ERC20 tx
        :param seed: seed address
        :param address: extending address
        :param page: page argument, which greater than 0
        :param kwargs: other query params
        :return: scrapy.Request object or None
        """
        url = 'http://api.etherscan.io/api?module=account&action=%s' \
              '&address=%s' \
              '&offset=10000' \
              '&page=%d' \
              '&apikey=%s' % (action, address, page, self.apikey_bucket.pop())
        if kwargs is not None:
            for k, v in kwargs.items():
                url += '&{}={}'.format(k, v)

        if self.addr_filter(address) is not None:
            yield scrapy.Request(
                url=url,
                method='GET',
                callback=self.parse,
                cb_kwargs={'action': action, 'seed': seed, 'address': address, 'page': page, **kwargs},
                dont_filter=True,
            )

    def addr_filter(self, address: str):
        """
        filter illegal address
        :param address: address needs crawling
        :return:
        """
        if address is None \
                or len(address) < 42:
            return None
        return address
