# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TxItem(scrapy.Item):
    seed = scrapy.Field()
    tx = scrapy.Field()


class TTRItem(scrapy.Item):
    seed = scrapy.Field()
    p = scrapy.Field()


class FirstOrderNetItem(scrapy.Item):
    seed = scrapy.Field()
    txs = scrapy.Field()


class CloseItem(scrapy.Item):
    seed = scrapy.Field()


class ErrorItem(scrapy.Item):
    seed = scrapy.Field()
