from pandas import DataFrame, read_csv
from os import remove, path
from time import sleep
from feature5_2 import Feature2
from time import sleep
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

DATA_FOLDER = '../MOEX-FX'
MONTHS = ['2018-03', '2018-04', '2018-05']

FILE_FOR_PRICE_STEPS = f'{DATA_FOLDER}/2018-03/OrderLog20180301.txt'

BEGINS = 100000000000

ENDS = {'USD000000TOD': '174500000000',
        'USD000UTSTOM': '235000000000',
        'EUR_RUB__TOD': '150000000000',
        'EUR_RUB__TOM': '235000000000',
        'EURUSD000TOM': '235000000000',
        'EURUSD000TOD': '150000000000'}

INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']

PRICE_STEPS = {key: 10000 for key in INSTRUMENTS}
RANGE = 50
SUB_RANGE = 5
WRITE_TIME = {'USD000000TOD': False,
              'USD000UTSTOM': False,
              'EUR_RUB__TOD': False,
              'EUR_RUB__TOM': False,
              'EURUSD000TOM': False,
              'EURUSD000TOD': False}
            
MAX_BAND = 10000000

def sort_dict(d, reverse):
    newdict = {}
    for key in sorted(d, reverse=reverse):
        newdict[key] = d[key]
    d = newdict
    return newdict


def find_price_step(prices):
    min = 100000
    prices = sorted(prices)
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i-1]
        if delta < min and delta > 0:
            min = delta
    return min


def fill_price_steps(filename):
    df = read_csv(filename, sep=',', index_col=0)
    df.drop(labels=['BUYSELL', 'TIME', 'ORDERNO',
                    'ACTION', 'VOLUME', 'TRADENO', 'TRADEPRICE'], axis=1)
    for instr in INSTRUMENTS:
        instr_df = df[df['SECCODE'] == instr]
        PRICE_STEPS[instr] = find_price_step(instr_df['PRICE'].tolist())


def open_files(folders, files):
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'a')
    return files_dict


class SpectrumDay:
    def __init__(self, path, files, feature_vwap, feature_spread):
        global WRITE_TIME
        for key in WRITE_TIME:
            WRITE_TIME[key] = False
        fill_price_steps(FILE_FOR_PRICE_STEPS)
        self.files = open_files(path, files)
        self.feature_vwap = feature_vwap
        self.feature_spread = feature_spread

    def append_to_file(self, instr, time, volumes):
        global WRITE_TIME
        f = self.files[instr]
        # bug here?
        if WRITE_TIME[instr]:
            f.write(f'{time}\n')
        WRITE_TIME[instr] = True
        f.write(f'{time},')
        for volume in volumes:
            f.write(f'{volume},')

    def append_last_time(self):
        for instr in INSTRUMENTS:
            f = self.files[instr]
            f.write(ENDS[instr])

    def close_files(self):
        self.feature_vwap.write_to_file()
        self.feature_spread.write_to_file()
        for f in self.files.values():
            f.close()

    # instr_dict: (price, volume)
    def one_day(self, instr_dicts, time, instr):
        def create_pdf(dic, reverse):
            # reverse is for buy_volumes
            volumes = {i: 0 for i in range(0, 10)}
            if not bool(dic):
                return volumes
            best_price = max(dic) if reverse else min(dic)
            mult = PRICE_STEPS[instr]*RANGE
            # split into 10 parts
            distribution = None
            if reverse:
                # distribution = {i: [dic[price] for price in dic.keys() if (
                #     price > best_price-(10-i)*mult/10 and price <= best_price-(9-i)*mult/10)] for i in range(9, -1, -1)}
                distribution = {i: [dic[price] for price in dic.keys() if (
                    price <= best_price-(i)*mult/10 and price > best_price-(i+1)*mult/10)] for i in range(0, 10)}
            else:
                distribution = {i: [dic[price] for price in dic.keys() if (
                    price >= best_price+i*mult/10 and price < best_price+(i+1)*mult/10)] for i in range(0, 10)}
            # sum over the volumes within each part
            for key in distribution:
                for volume in distribution[key]:
                    volumes[key] += volume
            return volumes

        def normalize_pdf(old_pdf, divisor=MAX_BAND):
            return old_pdf if divisor == 0 else {key: old_pdf[key]/divisor for key in old_pdf}

        # do it for buy
        buy_prices = sort_dict(instr_dicts['B'][instr], reverse=False)
        # reverse=True is for bids, so they will be from best price to worst price
        buy_volumes = create_pdf(buy_prices, reverse=True)

        # do it for sell
        sell_prices = sort_dict(instr_dicts['S'][instr], reverse=False)
        # reverse=False for asks, so they will be from best price to worst price
        sell_volumes = create_pdf(sell_prices, reverse=False)

        relative_buy_volumes = normalize_pdf(buy_volumes)
        relative_sell_volumes = normalize_pdf(sell_volumes)

        self.append_to_file(instr, time, list(
            relative_buy_volumes.values()) + list(relative_sell_volumes.values()))
        
        self.feature_vwap.run(instr, time, sell_prices.copy(), buy_prices.copy())
        self.feature_spread.run(instr, time, sell_prices.copy(), buy_prices.copy())
