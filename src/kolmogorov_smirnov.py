from math import sqrt, log

ALPHA = 0.975
K_ALPHA = sqrt(-0.5 * log((1 - ALPHA)/2))
CDF_LENGTH = 10


def coeff_d(n, m):
    return sqrt((n * m)/(n + m))


def kolmogorov_smirnov(cdf1, cdf2):
    if len(cdf1) == 0 or len(cdf2) == 0:
        return None
    # hypothesis = the samples have the same distribution
    dists = [0 for i in range(CDF_LENGTH)]
    for k in range(CDF_LENGTH):
        dists[k] = abs(cdf1[k] - cdf2[k])
    # if statistic exceeds the quantile K_alpha then we reject the hypothesis
    return False if sqrt(CDF_LENGTH) * max(dists) > K_ALPHA else True


class Test:
    def __init__(self, count_bids_cdf, count_asks_cdf, time_bids_cdf=[], time_asks_cdf=[]):
        self.count = {'bids': count_bids_cdf, 'asks': count_asks_cdf}
        self.time = {'bids': time_bids_cdf, 'asks': time_asks_cdf}
        self.tests_results = {'test1': self.test1(),
                              'test2': self.test2(),
                              'test3': self.test3(),
                              'test4': self.test4()}

    def get_results_day(self):
        return self.tests_results

    def test1(self):
        # count: bid vs ask
        bid = self.count['bids']
        ask = self.count['asks']
        return kolmogorov_smirnov(bid, ask)

    def test2(self):
        # time: bid vs ask
        bid = self.time['bids']
        ask = self.time['asks']
        return kolmogorov_smirnov(bid, ask)

    def test3(self):
        # count bid vs time ask
        bid = self.count['bids']
        ask = self.time['asks']
        return kolmogorov_smirnov(bid, ask)

    def test4(self):
        # time bid vs count ask
        bid = self.time['bids']
        ask = self.count['asks']
        return kolmogorov_smirnov(bid, ask)