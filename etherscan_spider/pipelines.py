# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os

from .items import TxItem, CloseItem, TTRItem, FirstOrderNetItem, TxsItem


class TxPipeline:
    def __init__(self):
        self.file_map = dict()
        self.fields = [
            'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed',
            'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
        ]
        self.closed_seed = set()

    def process_item(self, item, spider):
        output_dir = spider.output_dir
        if isinstance(item, TxItem):
            field_mask = spider.field_mask
            fields = list()
            for field in self.fields:
                if field not in field_mask:
                    fields.append(field)

            if not self.file_map.get(item['seed'], None):
                filename = os.path.join(output_dir, item['seed'].lower() + '.csv')
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
            filename = os.path.join(output_dir, item['seed'].lower() + '_ttr.csv')
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


class TxsPipeline:
    def __init__(self):
        self.cache_dir = None
        self.process_methods = {
            'external': self.process_external_txs,
            'internal': self.process_internal_txs,
            'erc20': self.process_erc20_txs,
            'erc721': self.process_erc721_txs,
        }
        self.tx_headers = {
            'external': [
                'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed', 'nonce',
                'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
            ],
            'internal': [
                'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasUsed', 'traceId',
                'isError', 'errCode', 'input', 'contractAddress', 'type'
            ],
            'erc20': [
                'hash', 'from', 'to', 'value', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed', 'nonce',
                'tokenSymbol', 'tokenDecimal', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
            ],
            'erc721': [
                'hash', 'from', 'to', 'tokenID', 'blockNumber', 'timeStamp', 'gas', 'gasPrice', 'gasUsed', 'nonce',
                'tokenSymbol', 'tokenDecimal', 'input', 'contractAddress', 'cumulativeGasUsed', 'confirmations'
            ],
        }

    def process_item(self, item, spider):
        if self.cache_dir is None:
            self.cache_dir = spider.cache_dir

        if not isinstance(item, TxsItem):
            return item

        if self.process_methods.get(item['tx_type']) is not None:
            process_method = self.process_methods[item['tx_type']]
            process_method(item)

    def process_external_txs(self, item: TxsItem):
        tdir = os.path.join(self.cache_dir, item['tx_type'])
        if not os.path.exists(tdir):
            os.makedirs(tdir)

        fn = os.path.join(tdir, item['address'] + '.csv')
        if not os.path.exists(fn):
            f = open(fn, 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(self.tx_headers['external'])
        else:
            f = open(fn, 'a', newline='')
            writer = csv.writer(f)

        headers = self.tx_headers['external']
        for tx in item['txs']:
            writer.writerow([tx[headers[i]] for i in range(len(headers))])
        f.close()

    def process_internal_txs(self, item: TxsItem):
        tdir = os.path.join(self.cache_dir, item['tx_type'])
        if not os.path.exists(tdir):
            os.makedirs(tdir)

        fn = os.path.join(tdir, item['address'] + '.csv')
        if not os.path.exists(fn):
            f = open(fn, 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(self.tx_headers['internal'])
        else:
            f = open(fn, 'a', newline='')
            writer = csv.writer(f)

        headers = self.tx_headers['internal']
        for tx in item['txs']:
            writer.writerow([tx[headers[i]] for i in range(len(headers))])
        f.close()

    def process_erc20_txs(self, item: TxsItem):
        tdir = os.path.join(self.cache_dir, item['tx_type'])
        if not os.path.exists(tdir):
            os.makedirs(tdir)

        fn = os.path.join(tdir, item['address'] + '.csv')
        if not os.path.exists(fn):
            f = open(fn, 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(self.tx_headers['erc20'])
        else:
            f = open(fn, 'a', newline='')
            writer = csv.writer(f)

        headers = self.tx_headers['erc20']
        for tx in item['txs']:
            writer.writerow([tx[headers[i]] for i in range(len(headers))])
        f.close()

    def process_erc721_txs(self, item: TxsItem):
        tdir = os.path.join(self.cache_dir, item['tx_type'])
        if not os.path.exists(tdir):
            os.makedirs(tdir)

        fn = os.path.join(tdir, item['address'] + '.csv')
        if not os.path.exists(fn):
            f = open(fn, 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(self.tx_headers['erc721'])
        else:
            f = open(fn, 'a', newline='')
            writer = csv.writer(f)

        headers = self.tx_headers['erc721']
        for tx in item['txs']:
            writer.writerow([tx[headers[i]] for i in range(len(headers))])
        f.close()

    def close_spider(self, spider):
        output_headers = ['tx_type']
        for header in self.tx_headers.values():
            for field in header:
                if field in output_headers:
                    continue
                output_headers.append(field)
        output_headers_idx = {output_headers[i]: i for i in range(len(output_headers))}

        output_dir = spider.output_dir
        for k, v in spider.seed_map.items():
            strategy = v['strategy']

            if not os.path.join(output_dir):
                os.makedirs(output_dir)
            out_fn = os.path.join(output_dir, k + '.csv')
            out_file = open(out_fn, 'w', newline='')
            out_writer = csv.writer(out_file)
            out_writer.writerow(output_headers)

            for address in strategy.vis:
                for tx_type in spider.tx_types:
                    txs_fn = os.path.join(spider.cache_dir, tx_type, address + '.csv')
                    if not os.path.exists(txs_fn):
                        continue
                    with open(txs_fn, 'r') as f:
                        reader = csv.reader(f)
                        headers = next(reader)
                        for row in reader:
                            _row = [None for _ in range(len(output_headers))]
                            _row[0] = tx_type
                            for i in range(len(row)):
                                idx = output_headers_idx[headers[i]]
                                _row[idx] = row[i]
                            out_writer.writerow(_row)
            out_file.close()
