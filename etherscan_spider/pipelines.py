# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os

from .items import SubgraphItem, TxItem, CloseItem


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
                self.file_map[item['seed']] = open(filename, 'a', newline='')
                csv.writer(self.file_map[item['seed']]).writerow(self.file_headers)

            csv.writer(self.file_map[item['seed']]).writerow([item['tx'][key] for key in self.file_headers])
        elif isinstance(item, CloseItem):
            self.file_map[item['seed']].close()
            del self.file_map[item['seed']]
        return item


class TxPipeline_:
    def __init__(self):
        self.data_path = './data'
        self.strategy_path = None

    def process_item(self, item, spider):
        if isinstance(item, SubgraphItem):
            if self.strategy_path is None:
                if not os.path.exists('%s/%s' % (self.data_path, spider.strategy)):
                    os.mkdir('%s/%s' % (self.data_path, spider.strategy))
                self.strategy_path = spider.strategy

            with open('%s/%s/%d_%s.csv' % (self.data_path, self.strategy_path, spider.epa, item['address']), 'w',
                      newline='') as f:
                writer = csv.writer(f)
                for e in item['edges']:
                    raw_data = e[2].get('raw_data')
                    writer.writerow([
                        raw_data['hash'],
                        raw_data['from'],
                        raw_data['to'],
                        raw_data['value'],
                        raw_data['blockNumber'],
                        raw_data['timeStamp'],
                        raw_data['gas'],
                        raw_data['gasPrice'],
                        raw_data['gasUsed'],
                    ])
        return item
