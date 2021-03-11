from pandas import read_csv
from os import path, remove, mkdir
from shutil import rmtree
from typing import List, Dict, Tuple
from time import sleep

PRICE_STEPS = {'USD000000TOD': 0.0025,
               'USD000UTSTOM': 0.0025,
               'EUR_RUB__TOD': 0.0025,
               'EUR_RUB__TOM': 0.0025,
               'EURUSD000TOM': 0.0001,
               'EURUSD000TOD': 0.0001}

INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM']

PERIODS = [1000000, 5000000, 15000000, 30000000, 60000000]

LIQUIDITY_VOLUMES = {instr: {'A': {}, 'B': {}} for instr in INSTRUMENTS}


def open_files(folders, instrs):
    dict_files = {'ASKS': {},
                  'BIDS': {}}
    for instr in instrs:
        if path.exists(f'{folders}/{instr}'):
            rmtree(f'{folders}/{instr}')
        mkdir(f'{folders}/{instr}')
    
    for f in instrs:
        dict_files['ASKS'][f] = open(f'{folders}/{f}/ASKS.txt', 'w')
        dict_files['BIDS'][f] = open(f'{folders}/{f}/BIDS.txt', 'w')
    return dict_files


def close_files(files_ba):
    for ab in files_ba:
        files = files_ba[ab]
        for f in files.values():
            f.close()


class Feature4:
    def __init__(self, dir: str):
        instrs = [instr for instr in INSTRUMENTS]
        self.files = open_files(dir, instrs)

    def write_to_file(self):
        for instr in INSTRUMENTS:
            f = self.files['ASKS'][instr]
            asks = LIQUIDITY_VOLUMES[instr]['A']
            for time in asks:
                f.write(f'{time},')
                for value in asks[time]:
                    f.write(f'{value},')
                f.write('\n')
            
            f = self.files['BIDS'][instr]            
            bids = LIQUIDITY_VOLUMES[instr]['B']
            for time in bids:
                f.write(f'{time},')
                for value in bids[time]:
                    f.write(f'{value},')
                f.write('\n')

            LIQUIDITY_VOLUMES[instr] = None
        close_files(self.files)

    def helper(self, row, orderlog, time: int, instr: str):
        if not instr in INSTRUMENTS:
            return

        bids = []
        asks = []
        for period in PERIODS:
            period_df = orderlog.loc[time - orderlog['TIME'] <= period]
            asks_volumes = (period_df.loc[period_df['BUYSELL'] == 'S'])[
                'VOLUME'].tolist()     
            bids_volumes = (period_df.loc[period_df['BUYSELL'] == 'B'])[
                'VOLUME'].tolist()

            asks_sum_volumes = sum(asks_volumes)
            bids_sum_volumes = sum(bids_volumes)

            asks.append(asks_sum_volumes)
            bids.append(bids_sum_volumes)

        LIQUIDITY_VOLUMES[instr]['A'][time] = asks
        LIQUIDITY_VOLUMES[instr]['B'][time] = bids

    def run(self, df_orderlog):
        df_orderlog = df_orderlog.drop(labels=['ORDERNO', 'PRICE', 'TRADEPRICE', 'TRADENO'], axis=1)
        df_orderlog = df_orderlog.loc[(df_orderlog['ACTION'] == 1)]

        df_orderlog.apply(lambda row: self.helper(row, df_orderlog, row['TIME'], row['SECCODE']), axis=1)
        self.write_to_file()