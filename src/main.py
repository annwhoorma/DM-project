from orderbook import one_day_orderbook
from spectrum import SpectrumDay
from pandas import read_csv
from os import listdir, mkdir, path
from day_average import AverageDay
from periods_average import TomAverageDay

INSTRUMENTS = ['USD000000TOD',
               'USD000UTSTOM',
               'EUR_RUB__TOD',
               'EUR_RUB__TOM',
               'EURUSD000TOD',
               'EURUSD000TOM']
DATA_FOLDER = '../MOEX-FX'

ORDERBOOKS_DATA_FOLDER = '../ORDERBOOKS'
SPECTRUM_DATA_FOLDER = '../SPECTRUM'
AVG_DATA_FOLDER = '../AVERAGE'
TOM_AVG_DATA_FOLDER = '../AVERAGE_TOM'

MONTHS = ['2018-03', '2018-04', '2018-05']
SPECTRUM_MONTHS = ['2018-03']

def remove_tradelogs(all_data):
    new_all_data = [[] for i in range(len(all_data))]
    for month_i in range(len(all_data)):
        for filik in all_data[month_i]:
            if not 'TradeLog' in filik:
                new_all_data[month_i].append(filik)
    return new_all_data


def create_res_dir(name):
    return name
    if not path.exists(name):
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
    dirname1 = f'{path_to}'
    dirname2 = f'{day}'
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


def main12():
    # tasks 1 and 2:
    all_data = [listdir(f'{DATA_FOLDER}/{month}') for month in MONTHS]
    all_data = remove_tradelogs(all_data)
    orderbooks_dir = create_res_dir(ORDERBOOKS_DATA_FOLDER)
    spectrum_dir = create_res_dir(SPECTRUM_DATA_FOLDER)

    for month_i in range(len(all_data)):
        for filename in all_data[month_i]:
            if filename != 'OrderLog20180301.txt':
                continue
            print(f'{filename}')
            read_from_folder = f'{DATA_FOLDER}/{MONTHS[month_i]}'
            orderbooks_save_to_month = f'{orderbooks_dir}/{MONTHS[month_i]}'
            spectrum_save_to_month = f'{spectrum_dir}/{MONTHS[month_i]}'

            df = read_csv(f'{read_from_folder}/{filename}', sep=',', index_col=0)
            files, instr_dicts, save_to_day = generate_files_names(filename, df.columns)

            spectrum = SpectrumDay(create_path(spectrum_save_to_month, save_to_day), files)
            one_day_orderbook(df, instr_dicts, files, create_path(orderbooks_save_to_month, save_to_day), spectrum)
            spectrum.close_files()
        

def main3():
    average_dir = create_res_dir(AVG_DATA_FOLDER)
    tom_average_dir = create_res_dir(TOM_AVG_DATA_FOLDER)
    tom_average_dir_var1 = create_res_dir(f'{tom_average_dir}/VAR1')
    tom_average_dir_var2 = create_res_dir(f'{tom_average_dir}/VAR2')
    spectrum_data = {month: listdir(f'{SPECTRUM_DATA_FOLDER}/{month}') for month in SPECTRUM_MONTHS}
    
    tom_average = TomAverageDay(tom_average_dir_var1, tom_average_dir_var2)

    for month in spectrum_data:
        days = spectrum_data[month]
        for day in days:
            path_to_spectrum = f'{SPECTRUM_DATA_FOLDER}/{month}/{day}/'
            average = AverageDay(create_path(f'{average_dir}/{month}', day), path_to_spectrum)
            average.run()
            
            tom_average.run(path_to_spectrum)

main12()
# main3()