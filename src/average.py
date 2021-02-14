from pandas import DataFrame, read_csv
from shutil import rmtree
from os import path, remove
from typing import List, Dict, Tuple
from kolmogorov_smirnov import Test
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']

BEGINS = 100000000000
ENDS = {'USD000000TOD': 174500000000,
        'USD000UTSTOM': 235000000000,
        'EUR_RUB__TOD': 150000000000,
        'EUR_RUB__TOM': 235000000000,
        'EURUSD000TOM': 235000000000,
        'EURUSD000TOD': 150000000000}

PATH = '../SPECTRUM/'
MONTHS = ['2018-03', '2018-04', '2018-05']


def open_files(folders, files):
    # copied from spectrum.py
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'w')
    return files_dict

def close_files(files):
    for f in files:
        f.close()

class AverageDay:
    def __init__(self, avg_dir, spectrum_path):
        files = {instr: f'{instr}.txt' for instr in INSTRUMENTS}
        global PATH
        PATH = spectrum_path
        self.files = open_files(avg_dir, files)

    def write_to_file(self, instr, avg_count_pdf, avg_time_pdf, count_cdf, time_cdf):
        f = self.files[instr]
        f.write("AVERAGE BY VALUE [BIDS] <PDF, CDF>:\n")
        for avcount in avg_count_pdf[:10]:
            f.write(f'{str(avcount)} ')
        f.write('\n')
        for count in count_cdf[:10]:
            f.write(f'{str(count)} ')

        f.write("\nAVERAGE BY VALUE [ASKS] <PDF, CDF>:\n")
        for avcount in avg_count_pdf[10:20]:
            f.write(f'{str(avcount)} ')
        f.write('\n')
        for count in count_cdf[10:20]:
            f.write(f'{str(count)} ')

        f.write("\nAVERAGE BY TIME [BIDS] <PDF, CDF>:\n")
        for avti in avg_time_pdf[:10]:
            f.write(f'{str(avti)} ')
        f.write('\n')
        for ti in time_cdf[:10]:
            f.write(f'{str(ti)} ')

        f.write("\nAVERAGE BY TIME [ASKS] <PDF, CDF>:\n")
        for avti in avg_time_pdf[10:20]:
            f.write(f'{str(avti)} ')
        f.write('\n')
        for ti in time_cdf[10:20]:
            f.write(f'{str(ti)} ')
        # f.close()

    def average_by_time(self, theta):
        columns = self.df.columns.tolist()
        columns.remove('time_diff')
        temp_df = DataFrame(columns=columns)
        for column in columns:
            temp_df[column] = (
                (self.df['time_diff'] * self.df[column]) / (theta - BEGINS)).values.tolist()
        return temp_df.sum(axis=0)

    def average_by_count(self):
        coeff = len(self.df.index)
        sums = self.df.sum(axis=0).tolist()
        res = []
        for s in sums:
            res.append(s/coeff)
        return res

    def create_cdf(self, pdfs):
        bids = pdfs[:10]
        asks = pdfs[10:20]
        bids_cdf = [0 for i in range(len(bids))]
        asks_cdf = [0 for i in range(len(asks))]
        for i in range(len(bids)):
            bids_cdf[i] = sum(bids[:i+1])
            asks_cdf[i] = sum(asks[:i])
        return bids_cdf + asks_cdf

    def run_tests(self, time_cdf, count_cdf):
        test = Test(count_cdf[:10], count_cdf[10:20], time_cdf[:10], time_cdf[10:20])
        print(test.get_results())
        return test.get_results()

    def save_tests(self, results, instr):
        f = self.files[instr]
        f.write('\nKolmogorov-Smirnov Test')
        f.write(f'\ncount: bid vs ask: {results["test1"]}')
        f.write(f'\ntime: bid vs ask: {results["test2"]}')
        f.write(f'\ncount bid vs time ask: {results["test3"]}')
        f.write(f'\ntime bid vs count ask: {results["test4"]}')        
        f.close()

    def run(self):
        for instr in INSTRUMENTS:
            df = read_csv(f'{PATH}{instr}.txt', sep=',', header=None)
            df['time_diff'] = (df[21]-df[0]).values.tolist()
            self.df = df.drop(labels=[0, 21], axis=1)

            avg_time_pdf = self.average_by_time(ENDS[instr])
            self.df = self.df.drop(labels=['time_diff'], axis=1)
            avg_count_pdf = self.average_by_count()
            time_cdf = self.create_cdf(avg_time_pdf)
            count_cdf = self.create_cdf(avg_count_pdf)
            self.write_to_file(instr, avg_count_pdf, avg_time_pdf, count_cdf, time_cdf)

            tests_results = self.run_tests(time_cdf, count_cdf)
            self.save_tests(tests_results, instr)

        close_files(self.files.values())