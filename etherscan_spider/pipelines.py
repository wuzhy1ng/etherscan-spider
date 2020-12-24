# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os

from .items import TxItem


class TxPipeline:
    def __init__(self):
        self.data_path = './data'
        self.strategy_path = None

    def process_item(self, item, spider):
        if isinstance(item, TxItem):
            if self.strategy_path is None:
                if not os.path.exists('%s/%s' % (self.data_path, spider.strategy)):
                    os.mkdir('%s/%s' % (self.data_path, spider.strategy))
                self.strategy_path = spider.strategy

            with open('%s/%s/%s.csv' % (self.data_path, self.strategy_path, item['address']), 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    item['raw_data']['hash'],
                    item['raw_data']['from'],
                    item['raw_data']['to'],
                    item['raw_data']['value'],
                    item['raw_data']['blockNumber'],
                    item['raw_data']['timeStamp'],
                    item['raw_data']['gas'],
                    item['raw_data']['gasPrice'],
                    item['raw_data']['gasUsed'],
                ])
        return item
