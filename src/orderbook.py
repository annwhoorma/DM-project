from pandas import DataFrame, read_csv
from shutil import rmtree
from typing import List, Dict, Tuple
from threading import Thread
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


def sort_dict(d):
    newdict = {}
    for key in sorted(d, reverse=True):
        newdict[key] = d[key]
    d = newdict
    return newdict


def save_orderbook(path, filenames, dicts):
    for instr in INSTRUMENTS:
        buy_dict = dicts['B'][instr]
        sell_dict = dicts['S'][instr]
        buy_dict, sell_dict = sort_dict(buy_dict), sort_dict(sell_dict)
        with open(f'{path}/{filenames[instr]}', 'w') as f:
            for key in sell_dict:
                # key - price, value - volume
                volume = dicts['S'][instr][key]
                f.write(f'S\t{key}\t{volume}\n')
            f.write('-----------------------------\n')
            for key in buy_dict:
                # key - price, value - volume
                volume = dicts['B'][instr][key]
                f.write(f'B\t{key}\t{volume}\n')


def helper_loop(instr_dicts: Dict[str, Dict[int, int]], instr: str, time: int,
                action: int, buysell: str, volume: int, price: int, spectrum_obj=None):
    if not instr in instr_dicts[buysell] or time > ENDS[instr]:
        return
    instr_dict = instr_dicts[buysell][instr]

    # check if this price already exists
    if price in instr_dict:
        if action == 0 or action == 2:
            instr_dict[price] -= volume
        elif action == 1:
            instr_dict[price] += volume
        # check volume size
        if instr_dict[price] <= 0:
            instr_dict.pop(price)
    elif (action == 1):
        instr_dict[price] = volume

    spectrum_obj.one_day(instr_dicts, time, instr)


def remove_non_positive_volumes(dicti):
    for instr in INSTRUMENTS:
        dicti['B'][instr] = {k: v for k,
                             v in dicti['B'][instr].items() if v > 0}
        dicti['S'][instr] = {k: v for k,
                             v in dicti['S'][instr].items() if v > 0}
    return dicti


def one_day_orderbook(df, instr_dicts, filenames, path, spectrum_obj=None):
    # create a folder for each day which will contain 6 instruments
    df = df.drop(labels=['ORDERNO', 'TRADENO', 'TRADEPRICE'], axis=1)
    df = df.loc[df['SECCODE'].isin(INSTRUMENTS)]

    df.apply(lambda row: helper_loop(instr_dicts, row['SECCODE'], int(row['TIME']), row['ACTION'], row['BUYSELL'],
                                     row['VOLUME'], row['PRICE'], spectrum_obj), axis=1)
    spectrum_obj.append_last_time()
    # instr_dicts = remove_non_positive_volumes(instr_dicts)
    # thread = Thread(target=save_orderbook, args=(path, filenames, instr_dicts))
    # thread.start()
    # thread.join()
