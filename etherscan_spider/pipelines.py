# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os

from .items import TxItem, CloseItem, TTRItem, FirstOrderNetItem


class TxPipeline:
    def __init__(self):
        self.file_map = dict()
        self.fields = [
            'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed',
            'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
        ]
        self.closed_seed = set()

    def process_item(self, item, spider):
        out_path = spider.out_path
        if not os.path.exists(out_path):
            os.mkdir(out_path)

        if isinstance(item, TxItem):
            field_mask = spider.field_mask
            fields = list()
            for field in self.fields:
                if field not in field_mask:
                    fields.append(field)

            if not self.file_map.get(item['seed'], None):
                filename = os.path.join(out_path, item['seed'].lower() + '.csv')
                self.file_map[item['seed']] = open(filename, 'w', newline='')
                csv.writer(self.file_map[item['seed']]).writerow(fields)

            csv.writer(self.file_map[item['seed']]).writerow([item['tx'][key] for key in fields])
        elif isinstance(item, CloseItem):
            if item['seed'] in self.closed_seed:
                return item

            # 记录爬过的地址
            self.closed_seed.add(item['seed'])
            with open('./data/crawled.csv', 'a', newline='') as f:
                csv.writer(f).writerow([item['seed'], ])
        elif isinstance(item, TTRItem):
            filename = os.path.join(out_path, item['seed'].lower() + '_ttr.csv')
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['address', 'weight'])
                for k, v in item['p'].items():
                    writer.writerow([k, v])
        elif isinstance(item, FirstOrderNetItem):
            if not os.path.exists(spider.cache_path):
                os.makedirs(spider.cache_path)

            filename = os.path.join(spider.cache_path, item['seed'] + '.csv')
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.fields)
                for tx in item['txs']:
                    writer.writerow([tx[key] for key in self.fields])
        return item
