from pandas import DataFrame, read_csv
from shutil import rmtree
from os import path, remove
from typing import List, Dict, Tuple
from kolmogorov_smirnov import kolmogorov_smirnov
from threading import Thread
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

INSTRUMENTS_TOM = ['USD000UTSTOM',
                   'EUR_RUB__TOM',
                   'EURUSD000TOM']

BEGINS = 100000000000
ENDS = {'USD000000TOD': 174500000000,
        'USD000UTSTOM': 235000000000,
        'EUR_RUB__TOD': 150000000000,
        'EUR_RUB__TOM': 235000000000,
        'EURUSD000TOM': 235000000000,
        'EURUSD000TOD': 150000000000}

MONTHS = ['2018-03', '2018-04', '2018-05']


TIME_PERIODS = [(100000000000, 150000000000),
                (150000000000, 190000000000),
                (190000000000, 235000000000)]

var1_columns = ['Day', '10:00 vs 15:00', '10:00 vs 19:00', '15:00 vs 19:00']
var2_columns = ['Day', '10:00-15:00', '15:00-19:00', '19:00-23:50']

CUR_DAY = 0

# day_number: [morn_vs_noon, morn_vs_eve, noon_vs_eve]
DATA_VAR1 = {instr: {key: [] for key in range(1, 2)} for instr in INSTRUMENTS_TOM}
# day_number: [10-15, 15-19, 19-23:50]
DATA_VAR2 = {instr: {key: [] for key in range(2, 3)} for instr in INSTRUMENTS_TOM}
PREV = {instr: {key: [] for key in range(1, 2)} for instr in INSTRUMENTS_TOM}

def open_files(folders, files, var):
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'a')
        f = files_dict[f]
        if var == 1:
            for col in var1_columns:
                f.write(f'{col},')
            f.write('\n')
        elif var == 2:
            for col in var2_columns:
                f.write(f'{col},')
            f.write('\n')

    return files_dict


def close_files(files):
    for f in files:
        f.close()


class TomAverageDay:
    def __init__(self, avg_dir1, avg_dir2):
        files = {instr: f'{instr}.txt' for instr in INSTRUMENTS_TOM}
        self.files_var1 = open_files(avg_dir1, files, var=1)
        self.files_var2 = open_files(avg_dir2, files, var=2)

    def average_by_count(self, df):
        coeff = len(df.index)
        sums = df.sum(axis=0).tolist()
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

    def do_one_df(self, df, instr, period):
        df = df.drop(labels=[0, 21], axis=1)

        avg_count_pdf = self.average_by_count(df)
        count_cdf = self.create_cdf(avg_count_pdf)
        return count_cdf[:10] + count_cdf[10:20]

    def compare_cdfs_pairwise(self, instr):
        global DATA_VAR1, DATA_VAR2
        cdfs = list(self.cdfs.values())
        mor = cdfs[0]
        noon = cdfs[1]
        eve = cdfs[2]
        # var 1
        mor_vs_noon = (kolmogorov_smirnov(mor[:10], noon[:10]), kolmogorov_smirnov(mor[10:20], noon[10:20]))
        mor_vs_eve = (kolmogorov_smirnov(mor[:10], eve[:10]), kolmogorov_smirnov(mor[10:20], eve[10:20]))
        noon_vs_eve = (kolmogorov_smirnov(noon[:10], eve[:10]), kolmogorov_smirnov(noon[10:20], eve[10:20]))
        DATA_VAR1[instr][CUR_DAY] = [mor_vs_noon, mor_vs_eve, noon_vs_eve]

        # save
        PREV[instr][CUR_DAY] = [mor, noon, eve]

        # var 2
        if CUR_DAY == 1:
            return
        prev_day_data = PREV[instr][CUR_DAY-1]
        prev_mor = prev_day_data[0]
        prev_noon = prev_day_data[1]
        prev_eve = prev_day_data[2]

        mor_vs_mor = (kolmogorov_smirnov(mor[:10], prev_mor[:10]), kolmogorov_smirnov(mor[10:20], prev_mor[10:20]))
        noon_vs_noon = (kolmogorov_smirnov(noon[:10], prev_noon[:10]), kolmogorov_smirnov(noon[10:20], prev_noon[10:20]))
        eve_vs_eve = (kolmogorov_smirnov(eve[:10], prev_eve[:10]), kolmogorov_smirnov(eve[10:20], prev_eve[10:20]))
        DATA_VAR2[instr][CUR_DAY] = [mor_vs_mor, noon_vs_noon, eve_vs_eve]

    def write_to_file(self):
        for instr in INSTRUMENTS_TOM:
            # var 1
            f = self.files_var1[instr]
            for day_number in DATA_VAR1[instr]:
                day = DATA_VAR1[instr][day_number]
                f.write(f'{day_number},{day[0]},{day[1]},{day[2]}\n')
            # var 2
            f = self.files_var2[instr]
            print(DATA_VAR2[instr])
            for day_number in DATA_VAR2[instr]:
                day = DATA_VAR2[instr][day_number]
                print(day)
                print(day[0])
                print(day[1])
                print(day[2])
                print()
                f.write(f'{day_number},{day[0]},{day[1]},{day[2]}\n')
        # close_files(list(self.files_var1.values()))
        # close_files(list(self.files_var2.values()))

    def run(self, spectrum_path):
        global CUR_DAY
        CUR_DAY += 1
        for instr in INSTRUMENTS_TOM:
            filename = f'{spectrum_path}{instr}.txt'
            with open(filename, 'r') as f:
                first_line = f.readline()
                if first_line == '' or first_line == '\n' or first_line[0] == ' ':
                    continue

            df = read_csv(filename, sep=',', header=None)
            
            self.period_dfs = {}
            self.cdfs = {}

            for time_period in TIME_PERIODS:
                start, end = time_period
                self.period_dfs[f'{start}-{end}'] = df.loc[(
                    df[0] >= start) & (df[0] < end)]

            for period in self.period_dfs:
                cur_df = self.period_dfs[period]
                self.cdfs[period] = self.do_one_df(cur_df, instr, period)
            self.compare_cdfs_pairwise(instr)
        
        if CUR_DAY == 64:
            self.write_to_file()
            close_files(list(self.files_var1.values()))
            close_files(list(self.files_var2.values()))
            print('finished files')