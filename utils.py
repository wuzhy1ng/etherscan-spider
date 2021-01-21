import sys

if __name__ == '__main__':
    if sys.argv[1] == 'export':
        sys.argv.pop(1)
        from etherscan_spider.utils.data_export import process

        process()
