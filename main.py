from pandas import DataFrame, read_csv, concat
from time import time
from os import listdir, mkdir, path
from shutil import rmtree

from typing import List, Dict, Tuple
import warnings
import numpy as np
warnings.simplefilter(action='ignore', category=FutureWarning)

DATA_FOLDER = 'MOEX-FX'
RES_DATA_FOLDER = 'ORDERBOOKS'
MONTHS = ['2018-03', '2018-04', '2018-05']


FILENAME = 'OrderLog202010.txt'
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


def generate_names(filename, headers):
    date = filename.split('OrderLog')[1].split('.')[0]
    files = {}
    lists = {'B': {}, 'S': {}}
    for instr in INSTRUMENTS:
        files[instr] = f'{instr}.txt'
        lists['B'][instr] = {}  # key - price, value - (volume)
        lists['S'][instr] = {}  # key - price, value - (volume)
    return files, lists, date


def sort_dict(d):
    newdict = {}
    for key in sorted(d, reverse=True):
        newdict[key] = d[key]
    return newdict

def save_files(save_to, save_to_day, filenames, dicts):
    dirname1 = f'{save_to}'
    dirname2 = f'{save_to_day}'
    if not path.exists(dirname1):
        mkdir(dirname1)
        mkdir(f'{dirname1}/{dirname2}')
    else:
        if not path.exists(f'{dirname1}/{dirname2}'):
            mkdir(f'{dirname1}/{dirname2}')

    for instr in INSTRUMENTS:
        buy_dict = dicts['B'][instr]
        sell_dict = dicts['S'][instr]
        buy_dict, sell_dict = sort_dict(buy_dict), sort_dict(sell_dict)
        with open(f'{dirname1}/{dirname2}/{filenames[instr]}', 'w') as f:
            for key in sell_dict:
                # key - price, value - volume
                volume = dicts['S'][instr][key]
                f.write(f'S\t{key}\t{volume}\n')
            f.write('-----------------------------\n')
            for key in buy_dict:
                # key - price, value - volume
                volume = dicts['B'][instr][key]
                f.write(f'B\t{key}\t{volume}\n')


def helper_loop(files: Dict[str, str], instr_dicts: Dict[str, Dict[int, Tuple[int]]], instr: str, time: int, action: int, buysell: str, volume: int, price: int):
    if not instr in instr_dicts[buysell] or time > ENDS[instr]:
        return
    instr_dict = instr_dicts[buysell][instr]
    
    # check if this price already exists
    if price in instr_dict:
        if action == 0 or action == 2:
            instr_dict[price] -= volume
        elif action == 1:
            instr_dict[price] += volume
        # check volume size - IN THE NEW VERSION I DO IT IN THE END
        if instr_dict[price] <= 0:
            instr_dict.pop(price)
    elif (action == 1):
        instr_dict[price] = volume


def remove_non_positive_volumes(dicti):
    for instr in INSTRUMENTS:
        dicti['B'][instr] = {k: v for k,
                             v in dicti['B'][instr].items() if v > 0}
        dicti['S'][instr] = {k: v for k,
                             v in dicti['S'][instr].items() if v > 0}
    return dicti


def one_day(filename, read_from_folder, save_to_month):
    # create a folder for each day which will contain 6 instruments
    print(f'{read_from_folder}/{filename}')
    df = read_csv(f'{read_from_folder}/{filename}',
                  sep=',', index_col=0)
    df = df.drop(labels=['ORDERNO', 'TRADENO', 'TRADEPRICE'], axis=1)

    files, instr_dicts, save_to_day = generate_names(filename, df.columns)

    df.apply(lambda row: helper_loop(files, instr_dicts,
                                     row['SECCODE'], row['TIME'], row['ACTION'], row['BUYSELL'],
                                     row['VOLUME'], row['PRICE']), axis=1)

    instr_dicts = remove_non_positive_volumes(instr_dicts)
    save_files(save_to_month, save_to_day, files, instr_dicts)


def remove_tradelogs(all_data):
    new_all_data = [[] for i in range(len(all_data))]
    for month_i in range(len(all_data)):
        for filik in all_data[month_i]:
            if not 'TradeLog' in filik:
                new_all_data[month_i].append(filik)
    return new_all_data


def create_res_dir():
    if not path.exists(RES_DATA_FOLDER):
        mkdir(RES_DATA_FOLDER)
        return RES_DATA_FOLDER

    new_name = RES_DATA_FOLDER + '_0'
    i = 0
    while path.exists(new_name):
        new_name = RES_DATA_FOLDER + '_' + str(i)
        i += 1
    mkdir(new_name)
    return new_name


def main():
    # CHANGE ALL_DATA TO DICTIONARY
    all_data = [listdir(f'{DATA_FOLDER}/{month}') for month in MONTHS]
    all_data = remove_tradelogs(all_data)
    res_dir = create_res_dir()

    for month_i in range(len(all_data)):
        for day in all_data[month_i]:
            one_day(
                day, f'{DATA_FOLDER}/{MONTHS[month_i]}', f'{res_dir}/{MONTHS[month_i]}')


main()
