# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TxItem(scrapy.Item):
    seed = scrapy.Field()
    tx = scrapy.Field()


class CloseItem(scrapy.Item):
    seed = scrapy.Field()


class ErrorItem(scrapy.Item):
    seed = scrapy.Field()
