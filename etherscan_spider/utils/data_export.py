import argparse
import csv
import os
import multiprocessing as mp


class BaseExporter:
    def __call__(self, *args, **kwargs):
        self.gen_data(args[0], args[1])

    def gen_data(self, in_filename: str, out_filename: str):
        in_file = open(in_filename, 'r')
        out_file = open(out_filename, 'w', newline='')

        reader = csv.reader(in_file)
        writer = csv.writer(out_file)

        # save header
        header = next(reader)
        writer.writerow(header)

        # save data
        s = set()
        for row in reader:
            if row[0] not in s:
                writer.writerow(row)
                s.add(row[0])

        in_file.close()
        out_file.close()

        # 删掉空文件
        if len(s) == 0:
            os.remove(out_filename)


class RandomExporter(BaseExporter):
    pass


class BFSExporter(BaseExporter):
    pass


class OPICHaircutExporter(BaseExporter):
    pass


def process():
    parser = argparse.ArgumentParser()
    parser.description = 'Clean raw data'
    # parser.add_argument('-m', '--method', help='crawl method(Random, BFS or OPICHaircut)', dest='method', type=str,
    #                     default=None)
    parser.add_argument('-i', '--input', help='input raw data folder', dest='input', type=str, default=None)
    parser.add_argument('-o', '--output', help='output data folder', dest='output', type=str, default=None)
    parser.add_argument('-c', '--crawled', help='output crawled seeds', dest='crawled', type=bool, default=False)

    args = parser.parse_args()

    if args.input is None or args.output is None:
        print('lost arguments')
        return

    if not os.path.exists(args.input):
        print('input folder doesn\'t existed ')
        return

    if not os.path.exists(args.output):
        os.mkdir(args.output)

    crawled_seeds = set()
    if args.crawled is True:
        with open('./data/crawled.csv', 'r') as f:
            for row in csv.reader(f):
                crawled_seeds.add(row[0])

    print('export using cpu core:', mp.cpu_count())
    pool = mp.Pool(mp.cpu_count())

    ept = BaseExporter()
    for filename in os.listdir(args.input):
        seed = filename.split('_')[1].split('.')[0]
        if args.crawled is True and seed not in crawled_seeds:
            continue

        print('processing file:%s' % filename)
        in_filename = args.input.rstrip('/|\\') + '/' + filename
        out_filename = args.output.rstrip('/|\\') + '/' + filename
        pool.apply_async(ept, (in_filename, out_filename))
    pool.close()
    pool.join()
