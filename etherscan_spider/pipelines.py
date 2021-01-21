# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os

from .items import TxItem, CloseItem


class TxPipeline:
    def __init__(self):
        self.data_path = './data'
        self.file_map = dict()
        self.file_headers = ['hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed']

    def process_item(self, item, spider):
        if isinstance(item, TxItem):
            epa = spider.epa
            if not os.path.exists('%s/%s' % (self.data_path, spider.strategy.name)):
                os.mkdir('%s/%s' % (self.data_path, spider.strategy.name))

            if not self.file_map.get(item['seed'], None):
                filename = '%s/%s/%s' % (self.data_path, spider.strategy.name, '%s_%s.csv' % (epa, item['seed']))
                self.file_map[item['seed']] = open(filename, 'w', newline='')
                csv.writer(self.file_map[item['seed']]).writerow(self.file_headers)

            csv.writer(self.file_map[item['seed']]).writerow([item['tx'][key] for key in self.file_headers])
        elif isinstance(item, CloseItem):
            # 记录爬过的地址
            with open('./data/crawled.csv', 'a', newline='') as f:
                csv.writer(f).writerow([item['seed'], ])
        return item
