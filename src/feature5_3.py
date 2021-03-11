# use tradelogs for this

from pandas import read_csv
from os import path, remove, mkdir
from shutil import rmtree
from time import sleep


BAND_VALUES = [100000, 200000, 500000, 1000000, 5000000, 10000000]
MAX_BAND = max(BAND_VALUES)

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

ENDS = {'USD000000TOD': 174500000000,
        'USD000UTSTOM': 235000000000,
        'EUR_RUB__TOD': 150000000000,
        'EUR_RUB__TOM': 235000000000,
        'EURUSD000TOM': 235000000000,
        'EURUSD000TOD': 150000000000}

PERIODS = [1000000, 5000000, 15000000, 30000000, 60000000]

AGGRESSOR_VOLUMES = {instr: {'A': {}, 'B': {}} for instr in INSTRUMENTS}


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


class Feature3:
    def __init__(self, dir):
        instrs = [instr for instr in INSTRUMENTS]
        self.files = open_files(dir, instrs)

    def write_to_file(self):
        for instr in INSTRUMENTS:
            # print(AGGRESSOR_VOLUMES[instr]['A'])
            f = self.files['ASKS'][instr]
            asks = AGGRESSOR_VOLUMES[instr]['A']
            for time in asks:
                f.write(f'{time},')
                for value in asks[time]:
                    f.write(f'{value},')
                f.write('\n')
            
            f = self.files['BIDS'][instr]
            bids = AGGRESSOR_VOLUMES[instr]['B']
            for time in bids:
                f.write(f'{time},')
                for value in bids[time]:
                    f.write(f'{value},')
                f.write('\n')

            AGGRESSOR_VOLUMES[instr] = None
        close_files(self.files)

    def helper(self, orderlog, tradelog, time, instr):
        if time > 235000000000:
            return

        asks, bids = [], []
        for period in PERIODS:
            period_df = tradelog.loc[tradelog['TIME'] - time <= period]

            # tradenumbers of asks to list
            asks_tradenumbers = (orderlog.loc[orderlog['BUYSELL'] == 'S'])[
                'TRADENO'].tolist()
            # create df for asks in this period
            asks_period_df = period_df.loc[period_df['TRADENO'].isin(asks_tradenumbers)]
            
            # tradenumbers of bids to list
            bids_tradenumbers = (orderlog.loc[orderlog['BUYSELL'] == 'B'])[
                'TRADENO'].tolist()
            # create df for bids in this period
            bids_period_df = period_df.loc[period_df['TRADENO'].isin(bids_tradenumbers)]

            asks_sum_volumes = asks_period_df['VOLUME'].sum()
            bids_sum_volumes = bids_period_df['VOLUME'].sum()

            asks.append(asks_sum_volumes)
            bids.append(bids_sum_volumes)
            
        AGGRESSOR_VOLUMES[instr]['A'][time] = asks
        AGGRESSOR_VOLUMES[instr]['B'][time] = bids

    def run(self, df_orderlog, df_tradelog):
        df_tradelog['TIME'] = df_tradelog['TIME'].map(lambda a: a * 10**6)
        """orderlogs are needed for bids-asks separation"""
        max_period = max(PERIODS)
        df_tradelog = df_tradelog.drop(
            labels=['BUYORDERNO', 'SELLORDERNO', 'PRICE'], axis=1)
        # keep tradeno and buysell
        df_orderlog = df_orderlog.drop(
            labels=['ORDERNO', 'PRICE', 'VOLUME', 'TRADEPRICE'], axis=1)

        df_orderlog = df_orderlog.loc[(df_orderlog['SECCODE'].isin(INSTRUMENTS)) & 
                                      (df_orderlog['ACTION'] == 2)]
        df_tradelog = df_tradelog.loc[df_tradelog['SECCODE'].isin(INSTRUMENTS)]

        df_orderlog = df_orderlog.drop(labels=['ACTION'], axis=1)

        df_orderlog.apply(lambda row: self.helper(df_orderlog.loc[(row['TIME'] - df_orderlog['TIME'] <= max_period)], 
                                                  df_tradelog.loc[(row['TIME'] - df_tradelog['TIME'] <= max_period)],
                                                  row['TIME'], row['SECCODE']), axis=1)
        
                                                  
        self.write_to_file()