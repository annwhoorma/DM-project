from pandas import DataFrame, read_csv
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
# MONTHS = ['2018-01', '2018-02']  # to test

INSTR_HEADERS = ['BUYSELL',
                 'PRICE',
                 'VOLUME']

FILENAME = 'OrderLog202010.txt'
INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB_TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']
BEGINS = 100000000000
ENDS = 235000000000


def generate_names(filename, headers):
    date = filename.split('OrderLog')[1].split('.')[0]
    files = {}
    lists = {}
    for instr in INSTRUMENTS:
        files[instr] = f'{date}-{instr}.txt'
        # lists[instr] = DataFrame(columns=INSTR_HEADERS)
        lists[instr] = {} # key - orderno, value - tuple (buysell, price, volume)
    return files, lists, date


def save_files(save_to, save_to_day, filenames, dfs):
    # create a folder
    dirname1 = f'{save_to}'
    dirname2 = f'{save_to_day}'
    if not path.exists(dirname1):
        mkdir(dirname1)
        mkdir(f'{dirname1}/{dirname2}')
    else:
        if not path.exists(f'{dirname1}/{dirname2}'):
            mkdir(f'{dirname1}/{dirname2}')
    for key in filenames:
        dfs[key].to_csv(f'{dirname1}/{dirname2}/{filenames[key]}')


def save_files2(save_to, save_to_day, filenames, dicts):
    # create a folder
    dirname1 = f'{save_to}'
    dirname2 = f'{save_to_day}'
    if not path.exists(dirname1):
        mkdir(dirname1)
        mkdir(f'{dirname1}/{dirname2}')
    else:
        if not path.exists(f'{dirname1}/{dirname2}'):
            mkdir(f'{dirname1}/{dirname2}')
    for key in filenames:
        cur = dicts[key]
        with open(f'{dirname1}/{dirname2}/{filenames[key]}', 'w') as f:
            for key in cur:
                bs, price, volume = cur[key]
                f.write(f'{bs}\t{price}\t{volume}\n') # should be a tuple (buysell, price, volume)


# def helper_loop(files: Dict[str, str], instr_dfs: Dict[str, DataFrame], time: int, instr: str, order_number: int, action: int, buysell: str, volume: int, price: int):
#     if not instr in instr_dfs:
#         return
#     if not (time >= BEGINS and time <= ENDS):
#         return
#     instr_df = instr_dfs[instr]

#     # check if this orderno already exists
#     if order_number in instr_df.index:
#         # update
#         if action == 0 or action == 2:
#             # instr_df.at[order_number, 'VOLUME'] -= volume
            
#         elif action == 1:
#             instr_df.at[order_number, 'VOLUME'] += volume
#         # check volume size
#         if instr_df.at[order_number, 'VOLUME'] == 0:
#             instr_df.drop(axis=0, index=order_number, inplace=True)
#     elif (action == 1):
#         # create a new row
#         instr_df.loc[order_number, ['BUYSELL', 'PRICE', 'VOLUME']] = [
#             buysell, price, volume]
#     else:
#         print('you got a bug')

def helper_loop(files: Dict[str, str], instr_dicts: Dict[str, Dict[int, Tuple[str, int, int]]], time: int, instr: str, order_number: int, action: int, buysell: str, volume: int, price: int):
    if not instr in instr_dicts:
        return
    if not (time >= BEGINS and time <= ENDS):
        return
    instr_dict = instr_dicts[instr]

    # check if this orderno already exists
    if order_number in instr_dict.keys():
        # update
        # YOU SHOULD CHECK THE PRICE AS WELL KIND OF
        cur_bs, cur_price, cur_vol = instr_dict[order_number]
        if action == 0:
            instr_dict[order_number] = (cur_bs, cur_price, cur_vol - volume)
        elif action == 1:
            instr_dict[order_number] = (cur_bs, cur_price, cur_vol + volume)
        elif action == 2:
            # match the price
            pass
        # check volume size
        if instr_dict[order_number][2] == 0:
            del instr_dict[order_number]
    elif (action == 1):
        # create a new row
        instr_dict[order_number] = (buysell, price, volume)
    else:
        print('you got a bug')


def one_day(filename, read_from_folder, save_to_month):
    # create a folder for each day which will contain 6 instruments
    print(f'{read_from_folder}/{filename}')
    df = read_csv(f'{read_from_folder}/{filename}',
                  sep=',', index_col=0)
    df = df.drop(labels=['TRADENO', 'TRADEPRICE'], axis=1)

    # files, instr_dfs, save_to_day = generate_names(filename, df.columns)
    files, isntr_dicts, save_to_day = generate_names(filename, df.columns)

    begin = time()

    # df.apply(lambda row: helper_loop(files, instr_dfs, int(row['TIME']),
    #                                  row['SECCODE'], row['ORDERNO'],
    #                                  row['ACTION'], row['BUYSELL'],
    #                                  row['VOLUME'], row['PRICE']), axis=1)
    df.apply(lambda row: helper_loop(files, isntr_dicts, int(row['TIME']),
                                     row['SECCODE'], row['ORDERNO'],
                                     row['ACTION'], row['BUYSELL'],
                                     row['VOLUME'], row['PRICE']), axis=1)

    print(f'IT TOOK {time() - begin}')
    # save_files(save_to_month, save_to_day, files, instr_dfs)
    save_files2(save_to_month, save_to_day, files, isntr_dicts)


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
