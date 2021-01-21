import argparse
import csv
import os


class BaseExporter:
    def gen_data(self, in_filename, out_filename):
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
    args = parser.parse_args()

    if args.input is None or args.output is None:
        print('lost arguments')
        exit(0)

    if not os.path.exists(args.input):
        print('input folder doesn\'t existed ')
        return

    if not os.path.exists(args.output):
        os.mkdir(args.output)

    ept = BaseExporter()
    for filename in os.listdir(args.input):
        print('processing file:%s' % filename)
        in_filename = args.input.rstrip('/|\\') + '/' + filename
        out_filename = args.output.rstrip('/|\\') + '/' + filename
        ept.gen_data(in_filename, out_filename)

    # if args.method == 'Random':
    #     ept = RandomExporter()
    #     for filename in os.listdir(args.input):
    #         logging.info('processing file:%s' % filename)
    #         in_filename = args.input.rstrip('/|\\') + '/' + filename
    #         out_filename = args.output.rstrip('/|\\') + '/' + filename
    #         ept.gen_data(in_filename, out_filename)
    # elif args.method == 'BFS':
    #     ept = BFSExporter()
    #     for filename in os.listdir(args.input):
    #         logging.info('processing file:%s' % filename)
    #         in_filename = args.input.rstrip('/|\\') + '/' + filename
    #         out_filename = args.output.rstrip('/|\\') + '/' + filename
    #         ept.gen_data(in_filename, out_filename)
    # elif args.method == 'OPICHaircut':
    #     ept = OPICHaircutExporter()
    #     for filename in os.listdir(args.input):
    #         logging.info('processing file:%s' % filename)
    #         in_filename = args.input.rstrip('/|\\') + '/' + filename
    #         out_filename = args.output.rstrip('/|\\') + '/' + filename
    #         ept.gen_data(in_filename, out_filename)
    # logging.info('finished!!!')
