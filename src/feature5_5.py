from pandas import read_csv
from os import path, remove
from numpy import nan
from time import sleep


BAND_VALUES = [100000, 200000, 500000, 1000000, 5000000, 10000000]

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

BA_SPREADS = {instr: {} for instr in INSTRUMENTS}

def open_files(folders, files):
    # copied from spectrum.py
    files_dict = {}
    for f in files:
        if path.exists(f'{folders}/{files[f]}'):
            remove(f'{folders}/{files[f]}')
        files_dict[f] = open(f'{folders}/{files[f]}', 'w')
    return files_dict


def close_files(files):
    for f in files.values():
        f.close()


class Feature5:
    def __init__(self, dir):
        files = {instr: f'{instr}.txt' for instr in INSTRUMENTS}
        self.files = open_files(dir, files)

    def normalize(self, ba_spread, instr):
        return ba_spread / PRICE_STEPS[instr]

    def write_to_file(self):
        for instr in INSTRUMENTS:
            f = self.files[instr]
            for time in BA_SPREADS[instr]:
                f.write(f'{time},{BA_SPREADS[instr][time]}\n')
        close_files(self.files)

    def run(self, instr, time, asks_dic, bids_dic):
        if not instr in INSTRUMENTS:
            return
        min_ask = min(asks_dic) if len(asks_dic) > 0 else 0
        max_bid = max(bids_dic) if len(bids_dic) > 0 else 0
        ba_spread = min_ask - max_bid
        ba_spread_norm = self.normalize(ba_spread, instr)

        BA_SPREADS[instr][time] = ba_spread_norm if ba_spread_norm > 0 else nan