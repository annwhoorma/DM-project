from pandas import DataFrame, read_csv
from os import remove, path
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

DATA_FOLDER = '../MOEX-FX'
RES_DATA_FOLDER = '../SPECTRUM'
MONTHS = ['2018-03', '2018-04', '2018-05']

FILE_FOR_PRICE_STEPS = f'{DATA_FOLDER}/2018-03/OrderLog20180301.txt'

INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']

PRICE_STEPS = {key: 10000 for key in INSTRUMENTS}
RANGE = 50
SUB_RANGE = 5


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
    def __init__(self, path, files):
        fill_price_steps(FILE_FOR_PRICE_STEPS)
        self.path = path
        self.files = open_files(path, files)

    def append_to_file(self, instr, time, volumes):
        f = self.files[instr]
        f.write(f'{time},')
        for volume in volumes:
            f.write(f'{volume},')
        f.write(',\n')

    def close_files(self):
        for f in self.files.values():
            f.close()

    # instr_dict: (price, volume)
    def one_day(self, instr_dicts, time, instr):
        def create_pdf(dic):
            volumes = {i: 0 for i in range(0, 10)}
            if not bool(dic):
                return volumes
            limit_sell = min(dic)
            mult = PRICE_STEPS[instr]*RANGE
            # split into 10 parts
            distribution = {i: [price for price in dic.keys() if (
                price >= limit_sell+i*mult/10 and price < limit_sell+(i+1)*mult/10)] for i in range(0, 10, 1)}
            # sum over the volumes within each part
            for part in distribution:
                for price in distribution[part]:
                    volumes[part] += price
            return volumes

        def normalize_pdf(old_pdf, divisor):
            return old_pdf if divisor == 0 else {key: old_pdf[key]/divisor for key in old_pdf}

        # do it for buy
        buy_prices = sort_dict(instr_dicts['B'][instr], reverse=True)
        buy_volumes = create_pdf(buy_prices)
        FULL_BUY_VOLUME = sum(buy_volumes.values())

        # do it for sell
        sell_prices = sort_dict(instr_dicts['S'][instr], reverse=False)
        sell_volumes = create_pdf(sell_prices)
        FULL_SELL_VOLUME = sum(sell_volumes.values())

        relative_buy_volumes = normalize_pdf(buy_volumes, FULL_BUY_VOLUME)
        relative_sell_volumes = normalize_pdf(sell_volumes, FULL_SELL_VOLUME)

        self.append_to_file(instr, time, list(
            relative_buy_volumes.values()) + list(relative_sell_volumes.values()))
