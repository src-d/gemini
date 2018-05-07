"""
Calculate number of hashtabels and size of a band
using the threshold and sample size of wmh
"""

import argparse

from scipy.integrate import quad as integrate

def _false_positive_probability(threshold, b, r):
    def _probability(s):
        return 1 - (1 - s**float(r))**float(b)
    a, err = integrate(_probability, 0.0, threshold)
    return a


def _false_negative_probability(threshold, b, r):
    def _probability(s):
        return 1 - (1 - (1 - s**float(r))**float(b))
    a, err = integrate(_probability, threshold, 1.0)
    return a


def calc_hashtable_params(threshold, sample_size, false_positive_weight=0.5,
                          false_negative_weight=0.5):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative.
    :return: tuple(number of hashtables, size of each band).
    """
    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, sample_size + 1):
        max_r = int(sample_size / b)
        for r in range(1, max_r+1):
            fp = _false_positive_probability(threshold, b, r)
            fn = _false_negative_probability(threshold, b, r)
            error = fp*false_positive_weight + fn*false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--threshold",
        type=float,
        required=True)
    parser.add_argument(
        "-s",
        "--sample-size",
        type=int,
        required=True)

    args = parser.parse_args()
    htnum, band_size = calc_hashtable_params(args.threshold, args.sample_size)
    print("htnum", htnum)
    print("band_size", band_size)
