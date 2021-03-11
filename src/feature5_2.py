from pandas import read_csv
from os import path, remove, mkdir
from shutil import rmtree
from numpy import nan


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

VWAPs = {instr: {'A': {}, 'B': {}} for instr in INSTRUMENTS}


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


class Feature2:
    def __init__(self, dir):
        instrs = [instr for instr in INSTRUMENTS]
        self.files = open_files(dir, instrs)

    def normalize_vwap(self, instr, vwap, midpoint, band):
        return vwap * abs(midpoint - vwap) / PRICE_STEPS[instr]

    def calc_mid_point(self, min_ask, max_bid):
        return (min_ask - max_bid)/2

    def calc_vwap(self, band, orders, bids):
        if len(orders) == 0:
            return 0

        used = {}
        while band > 0 and len(orders) > 0:
            price = max(orders.keys()) if bids else min(orders.keys())
            volume = orders[price]
            if volume >= band:
                used[price] = band
                orders[price] -= band
                band = 0  # => end
            else:
                used[price] = volume
                orders[price] = 0
                orders.pop(price)
                band -= volume

        if len(orders) == 0 and band > 0:
            # means that we didnt fill the requested band value and ran out of volumes
            return nan

        vwap = 0
        for price in used:
            vwap += price * used[price]
        return vwap

    def write_to_file(self):
        for instr in INSTRUMENTS:
            f = self.files['ASKS'][instr]
            for time in VWAPs[instr]['A']:
                f.write(f'{time},')
                for value in VWAPs[instr]['A'][time]:
                    f.write(f'{value},')
                f.write('\n')
            
            f = self.files['BIDS'][instr]
            for time in VWAPs[instr]['B']:
                f.write(f'{time},')
                for value in VWAPs[instr]['B'][time]:
                    f.write(f'{value},')
                f.write('\n')
        close_files(self.files)

    def run(self, instr, time, asks_dic, bids_dic):
        if not instr in INSTRUMENTS:
            return
        min_ask = min(asks_dic) if len(asks_dic) > 0 else 0
        max_bid = max(bids_dic) if len(bids_dic) > 0 else 0
        if min_ask == 0 or max_bid == 0:
            VWAPs[instr]['A'][time] = [nan for i in range(len(BAND_VALUES))]
            VWAPs[instr]['B'][time] = [nan for i in range(len(BAND_VALUES))]
            return

        mp = self.calc_mid_point(min_ask, max_bid)
        asks_vwaps = []
        bids_vwaps = []
        for band in BAND_VALUES:
            asks_vwap = self.calc_vwap(band, asks_dic.copy(), bids=False)
            bids_vwap = self.calc_vwap(band, bids_dic.copy(), bids=True)

            norm_asks_vwap = self.normalize_vwap(instr, asks_vwap, mp, band)
            norm_bids_vwap = self.normalize_vwap(instr, bids_vwap, mp, band)

            asks_vwaps.append(norm_asks_vwap)
            bids_vwaps.append(norm_bids_vwap)

        VWAPs[instr]['A'][time] = asks_vwaps
        VWAPs[instr]['B'][time] = bids_vwaps
