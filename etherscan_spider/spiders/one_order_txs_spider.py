import scrapy


class OneOrderTxsSpiderSpider(scrapy.Spider):
    name = 'one_order_txs_spider'
    allowed_domains = ['*']

    def parse(self, response):
        pass
