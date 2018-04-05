#!/usr/bin/env python3

from datasketch import WeightedMinHashGenerator, WeightedMinHash
import numpy
import os

dirname = os.path.dirname(__file__)

def write_csv_int(name, arr):
    numpy.savetxt("%s/csv/%s" % (dirname, name), arr, fmt="%u", delimiter=",")

def write_csv_float(name, arr):
    numpy.savetxt("%s/csv/%s" % (dirname, name), arr, delimiter=",")

def tiny_test():
    v1 = [1, 0, 0, 0, 3, 4, 5, 0, 0, 0, 0, 6, 7, 8, 0, 0, 0, 0, 0, 0, 9, 10, 4]
    v2 = [2, 0, 0, 0, 4, 3, 8, 0, 0, 0, 0, 4, 7, 10, 0, 0, 0, 0, 0, 0, 9, 0, 0]

    bgen = WeightedMinHashGenerator(len(v1), 128, 1)

    write_csv_float("tiny-rs.csv", bgen.rs)
    write_csv_float("tiny-ln_cs.csv", bgen.ln_cs)
    write_csv_float("tiny-betas.csv", bgen.betas)

    write_csv_int("tiny-data.csv", [v1, v2])
    write_csv_int("tiny-hashes-0.csv", bgen.minhash(v1).hashvalues)
    write_csv_int("tiny-hashes-1.csv", bgen.minhash(v2).hashvalues)

def big_test():
    numpy.random.seed(0)
    data = numpy.random.randint(0, 100, (6400, 130))
    mask = numpy.random.randint(0, 5, data.shape)
    data *= (mask >= 4)
    del mask
    bgen = WeightedMinHashGenerator(data.shape[-1])

    write_csv_float("big-rs.csv", bgen.rs)
    write_csv_float("big-ln_cs.csv", bgen.ln_cs)
    write_csv_float("big-betas.csv", bgen.betas)

    write_csv_int("big-data.csv", data)

    c = 0
    for line in data:
        write_csv_int("big-hashes-%d.csv" % c, bgen.minhash(line).hashvalues)
        c = c+1

os.makedirs("%s/csv" % dirname, exist_ok=True)

tiny_test()
big_test()
