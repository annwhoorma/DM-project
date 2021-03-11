from pandas import DataFrame, read_csv
from shutil import rmtree
from os import path, remove
from typing import List, Dict, Tuple
from kolmogorov_smirnov import Test
from threading import Thread
from typing import List, Dict, Tuple
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


def open_files(folders: str, files: Dict[str, str]):
    # copied from spectrum.py
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'w')
    return files_dict

def close_files(files: List):
    for f in files:
        f.close()

class AverageDay:
    def __init__(self, avg_dir: str, spectrum_path: str):
        files = {instr: f'{instr}.txt' for instr in INSTRUMENTS}
        global PATH
        PATH = spectrum_path
        self.files = open_files(avg_dir, files)

    def write_to_file(self, instr: str, avg_count_pdf: List, avg_time_pdf: List, count_cdf: List, time_cdf: List, period: str):
        f = self.files[instr]
        if period=='all':
            f.write(f'ENTIRE DAY:')
        else:
            f.write(f'\n\nPERIOD: {period}')

        f.write("\nAVERAGE BY VALUE [BIDS] <PDF, CDF>:\n")
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

    def average_by_time(self, df, theta):
        columns = df.columns.tolist()
        columns.remove('time_diff')
        temp_df = DataFrame(columns=columns)
        for column in columns:
            temp_df[column] = (
                (df['time_diff'] * df[column]) / (theta - BEGINS)).values.tolist()
        return temp_df.sum(axis=0)

    def average_by_count(self, df: DataFrame):
        coeff = len(df.index)
        sums = df.sum(axis=0).tolist()
        res = []
        for s in sums:
            res.append(s/coeff)
        return res

    def create_cdf(self, pdfs: List):
        bids = pdfs[:10]
        asks = pdfs[10:20]
        bids_cdf = [0 for i in range(len(bids))]
        asks_cdf = [0 for i in range(len(asks))]
        for i in range(len(bids)):
            bids_cdf[i] = sum(bids[:i+1])
            asks_cdf[i] = sum(asks[:i])
        return bids_cdf + asks_cdf

    def run_tests(self, time_cdf: List, count_cdf: List):
        test = Test(count_cdf[:10], count_cdf[10:20], time_cdf[:10], time_cdf[10:20])
        return test.get_results_day()

    def save_tests(self, results, instr):
        f = self.files[instr]
        f.write('\n\n\nKolmogorov-Smirnov Test')
        f.write(f'\ncount: bid vs ask: {results["test1"]}')
        f.write(f'\ntime: bid vs ask: {results["test2"]}')
        f.write(f'\ncount bid vs time ask: {results["test3"]}')
        f.write(f'\ntime bid vs count ask: {results["test4"]}')

    def do_one_df(self, df: DataFrame, instr: str, period: str):
        df['time_diff'] = (df[21]-df[0]).values.tolist()
        df = df.drop(labels=[0, 21], axis=1)

        avg_time_pdf = self.average_by_time(df, ENDS[instr])
        df = df.drop(labels=['time_diff'], axis=1)
        avg_count_pdf = self.average_by_count(df)
        time_cdf = self.create_cdf(avg_time_pdf)
        count_cdf = self.create_cdf(avg_count_pdf)

        thread = Thread(target=self.write_to_file, args=(instr, avg_count_pdf, avg_time_pdf, count_cdf, time_cdf, period))
        thread.start()
        thread.join()

        tests_results = self.run_tests(time_cdf, count_cdf)
        self.save_tests(tests_results, instr)

    def run(self, TOM_instrs=False, time_periods=[]):
        for instr in INSTRUMENTS:
            if TOM_instrs and ENDS[instr] != 235000000000:
                continue

            filename = f'{PATH}/{instr}.txt'
            with open(filename, 'r') as f:
                first_line = f.readline()
                if first_line == '' or first_line == '\n' or first_line[0] == ' ':
                    continue

            df = read_csv(filename, sep=',', header=None)

            self.period_dfs = {'all': df}

            if len(time_periods) > 0:
                for time_period in time_periods:
                    start, end = time_period
                    self.period_dfs[f'{start}-{end}'] = df.loc[(df[0] >= start) & (df[0] < end)]
            
            for period in self.period_dfs:
                cur_df = self.period_dfs[period]
                self.do_one_df(cur_df, instr, period)

        close_files(self.files.values())