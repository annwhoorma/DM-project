from pandas import DataFrame, read_csv
from shutil import rmtree
from os import path, remove
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


def open_files(folders, files):
    # copied from spectrum.py
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'w')
    return files_dict


class AverageDay:
    def __init__(self, avg_dir, spectrum_path):
        files = {instr: f'{instr}.txt' for instr in INSTRUMENTS}
        global PATH
        PATH = spectrum_path
        self.files = open_files(avg_dir, files)

    def write_to_file(self, instr, avg_amount, avg_time):
        f = self.files[instr]
        f.write("AVERAGE BY VALUE [BIDS]:\n")
        for avam in avg_amount[:10]:
            f.write(f'{str(avam)} ')
        f.write("\nAVERAGE BY VALUE [ASKS]:\n")
        for avam in avg_amount[10:20]:
            f.write(f'{str(avam)} ')
        f.write("\nAVERAGE BY TIME [BIDS]:\n")
        for avti in avg_time[:10]:
            f.write(f'{str(avti)} ')
        f.write("\nAVERAGE BY TIME [ASKS]:\n")
        for avti in avg_time[10:20]:
            f.write(f'{str(avti)} ')
        f.close()

    def average_by_time(self, theta):
        columns = self.df.columns.tolist()
        columns.remove('time_diff')
        temp_df = DataFrame(columns=columns)
        for column in columns:
            temp_df[column] = (
                (self.df['time_diff'] * self.df[column]) / (theta - BEGINS)).values.tolist()
        return temp_df.sum(axis=0)

    def average_by_amount(self):
        coeff = len(self.df.index)
        sums = self.df.sum(axis=0).tolist()
        res = []
        for s in sums:
            res.append(s/coeff)
        return res

    def run(self):
        for instr in INSTRUMENTS:
            df = read_csv(f'{PATH}{instr}.txt', sep=',', header=None)
            df['time_diff'] = (df[21]-df[0]).values.tolist()
            self.df = df.drop(labels=[0, 21], axis=1)

            avg_time_res = self.average_by_time(ENDS[instr])
            self.df = self.df.drop(labels=['time_diff'], axis=1)
            avg_am_res = self.average_by_amount()
            self.write_to_file(instr, avg_am_res, avg_time_res)
