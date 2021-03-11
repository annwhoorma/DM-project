from orderbook import one_day_orderbook
from spectrum import SpectrumDay
from pandas import read_csv
from os import listdir, mkdir, path
from shutil import rmtree
from day_average import AverageDay
from feature5_2 import Feature2
from feature5_3 import Feature3
from feature5_4 import Feature4
from feature5_5 import Feature5
from periods_average import TomAverageDay
from multiprocessing import Process, Pool
from time import time

INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']
DATA_FOLDER = '../MOEX-FX'

ORDERBOOKS_DATA_FOLDER = '../OUTPUT/ORDERBOOKS'
SPECTRUM_DATA_FOLDER = '../OUTPUT/SPECTRUM' # feature1
AVG_DATA_FOLDER = '../OUTPUT/AVERAGE'
TOM_AVG_DATA_FOLDER = '../OUTPUT/AVERAGE_TOM'
FEATURE2_DATA = '../OUTPUT/F2_VWAP'
FEATURE3_DATA = '../OUTPUT/F3_AGGRESSORS'
FEATURE4_DATA = '../OUTPUT/F4_LIQUIDITY_REPL'
FEATURE5_DATA = '../OUTPUT/F5_SPREAD'

MONTHS = ['2018-03', '2018-04', '2018-05']


def remove_logs(all_data, remove_tradelogs=True):
    pattern = 'TradeLog' if remove_tradelogs else 'OrderLog'
    new_all_data = [[] for i in range(len(all_data))]
    for month_i in range(len(all_data)):
        for filik in all_data[month_i]:
            if not pattern in filik:
                new_all_data[month_i].append(filik)
    return new_all_data


def create_res_dir(name):
    if name == ORDERBOOKS_DATA_FOLDER or name == TOM_AVG_DATA_FOLDER:
        return name
    if not path.exists(name):
        mkdir(name)
        return name
    else:
        rmtree(name)
        mkdir(name)
    return name

    new_name = name + '_0'
    i = 0
    while path.exists(new_name):
        new_name = name + '_' + str(i)
        i += 1
    mkdir(new_name)
    return new_name


def create_path(path_to, day):
    dirname1 = path_to
    dirname2 = day
    if not path.exists(dirname1):
        mkdir(dirname1)
        mkdir(f'{dirname1}/{dirname2}')
    else:
        if not path.exists(f'{dirname1}/{dirname2}'):
            mkdir(f'{dirname1}/{dirname2}')
    return(f'{dirname1}/{dirname2}')


def generate_files_names(filename, headers):
    date = filename.split('OrderLog')[1].split('.')[0]
    files = {}
    lists = {'B': {}, 'S': {}}
    for instr in INSTRUMENTS:
        files[instr] = f'{instr}.txt'
        lists['B'][instr] = {}  # key - price, value - (volume)
        lists['S'][instr] = {}  # key - price, value - (volume)
    return files, lists, date

def process_file(filename):
    orderbooks_dir = ORDERBOOKS_DATA_FOLDER
    spectrum_dir = SPECTRUM_DATA_FOLDER
    feature2_dir = FEATURE2_DATA
    feature5_dir = FEATURE5_DATA
    print(f'started  {filename}')
    read_from_folder = f'{DATA_FOLDER}/{MONTHS[MONTH_I]}'
    orderbooks_save_to_month = f'{orderbooks_dir}/{MONTHS[MONTH_I]}'
    spectrum_save_to_month = f'{spectrum_dir}/{MONTHS[MONTH_I]}'
    feature2_save_to_month = f'{feature2_dir}/{MONTHS[MONTH_I]}'
    feature5_save_to_month = f'{feature5_dir}/{MONTHS[MONTH_I]}'

    df = read_csv(f'{read_from_folder}/{filename}', sep=',', index_col=0)
    files, instr_dicts, save_to_day = generate_files_names(filename, df.columns)

    feature2 = Feature2(create_path(feature2_save_to_month, save_to_day))
    feature5 = Feature5(create_path(feature5_save_to_month, save_to_day))

    spectrum = SpectrumDay(create_path(spectrum_save_to_month, save_to_day), files, feature2, feature5)
    one_day_orderbook(df, instr_dicts, files, create_path(orderbooks_save_to_month, save_to_day), spectrum)
    spectrum.close_files()
    print(f'finished {filename}')

# orderbook and spectrum (== feature 1) + feature 2, 5
def main12():
    # tasks 1 and 2:
    all_data = [listdir(f'{DATA_FOLDER}/{month}') for month in MONTHS]
    all_data = remove_logs(all_data)
    orderbooks_dir = create_res_dir(ORDERBOOKS_DATA_FOLDER)
    spectrum_dir = create_res_dir(SPECTRUM_DATA_FOLDER)
    feature2_dir = create_res_dir(FEATURE2_DATA)
    feature5_dir = create_res_dir(FEATURE5_DATA)

    for month_i in range(len(all_data)):
        global MONTH_I
        MONTH_I = month_i
        all_data[month_i] = sorted(all_data[month_i])
        with Pool(7) as pool:
            print("started smt")
            pool.map(process_file, all_data[month_i])


def lr_process_file(filename):
    print(f'{filename}')
    feature4_dir = FEATURE4_DATA
    month_i = MONTH_I
    read_from_folder = f'{DATA_FOLDER}/{MONTHS[month_i]}'
    feature4_save_to_month = f'{feature4_dir}/{MONTHS[month_i]}'

    df = read_csv(f'{read_from_folder}/{filename}', sep=',', index_col=0)
    _, _, save_to_day = generate_files_names(filename, df.columns)
    feature4 = Feature4(create_path(feature4_save_to_month, save_to_day))
    feature4.run(df)

# feature 4
def liquidity_replenishment():
    all_data = [listdir(f'{DATA_FOLDER}/{month}') for month in MONTHS]
    orderlogs = remove_logs(all_data)
    feature4_dir = create_res_dir(FEATURE4_DATA)

    for month_i in range(len(all_data)):
        global MONTH_I
        MONTH_I = month_i
        all_data[month_i] = sorted(orderlogs[month_i])
        with Pool(7) as pool:
            print("started smt")
            pool.map(process_file, orderlogs[month_i])


# averages - needs spectra
def main3():
    def process_folder(spectrum_data, month_i):
        spectrum_data = sorted(spectrum_data)
        spectrum_data[month_i] = sorted(spectrum_data[month_i])
        for day in spectrum_data[month_i]:
            if not '20180301' in day:
                continue
            print(day)
            path_to_spectrum = f'{SPECTRUM_DATA_FOLDER}/{MONTHS[month_i]}/{day}/'
            average = AverageDay(create_path(f'{average_dir}/{MONTHS[month_i]}', day), path_to_spectrum)
            average.run()
            
            tom_average.run(path_to_spectrum)   
    

    average_dir = create_res_dir(AVG_DATA_FOLDER)
    tom_average_dir = create_res_dir(TOM_AVG_DATA_FOLDER)
    tom_average_dir_var1 = create_res_dir(f'{tom_average_dir}/VAR1')
    tom_average_dir_var2 = create_res_dir(f'{tom_average_dir}/VAR2')
    spectrum_data = [listdir(f'{SPECTRUM_DATA_FOLDER}/{month}') for month in MONTHS]
    
    tom_average = TomAverageDay(tom_average_dir_var1, tom_average_dir_var2)

    for month_i in range(len(spectrum_data)):
        process_folder(spectrum_data, month_i)


# feature 3 (process aggressive trades)
def main4():
    def process_folder(folder, orderlogs, tradelogs, month_i):
        folder[month_i] = sorted(folder[month_i])
        orderlogs[month_i] = sorted(orderlogs[month_i])
        tradelogs[month_i] = sorted(tradelogs[month_i])
        for filename in orderlogs[month_i]:
            print(f'started  {filename}')
            read_from_folder = f'{DATA_FOLDER}/{MONTHS[month_i]}'
            feature3_save_to_month = f'{feature3_dir}/{MONTHS[month_i]}'

            date = filename[8:16]
            tradelog_filename = f'TradeLog{date}.txt'
            orderlog_df = read_csv(f'{read_from_folder}/{filename}', sep=',', index_col=0)
            tradelog_df = read_csv(f'{read_from_folder}/{tradelog_filename}', sep=',')

            _, _, save_to_day = generate_files_names(filename, orderlog_df.columns)

            feature3 = Feature3(create_path(feature3_save_to_month, save_to_day))
            feature3.run(orderlog_df, tradelog_df)
            print(f'finished {filename}')

    all_data = [listdir(f'{DATA_FOLDER}/{month}') for month in MONTHS]
    orderlogs = remove_logs(all_data, remove_tradelogs=True)
    tradelogs = remove_logs(all_data, remove_tradelogs=False)
    feature3_dir = create_res_dir(FEATURE3_DATA)

    processes = []
    for month_i in range(len(all_data)):
        process = Process(target=process_folder, args=(all_data, orderlogs, tradelogs, month_i))
        processes.append(process)

    for process in processes:
        process.start()
    
    for process in processes:
        process.join()


main12()
print('finished main12')
liquidity_replenishment()
print("finished lr")
# main4()
# print('finished main4')