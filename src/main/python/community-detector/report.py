import argparse

import numpy
import pyarrow.parquet as pq

import community_detector


def read_connected_components(filepath):
    dict = pq.read_table(filepath).to_pydict()

    ccs = dict['cc']
    ids = dict['element_ids']
    return list(zip(ccs, ids))


def read_buckets_matrix(filepath):
    dict = pq.read_table(filepath).to_pydict()

    id_to_buckets = dict['buckets']

    return community_detector.build_matrix(id_to_buckets)


def main(dirpath):
    connected_components = read_connected_components('%s/cc.parquet' % dirpath)

    buckets_matrix = read_buckets_matrix('%s/buckets.parquet' % dirpath)
    n_ids = buckets_matrix.shape[0]

    # TODO (carlosms): Scala produces a map of cc->element-id,
    # the lib requires element-id->cc, but only to convert it
    # to cc->element-id. Easy change once everything is working.
    id_to_cc = community_detector.build_id_to_cc(connected_components, n_ids)

    result = community_detector.detect_communities(id_to_cc, buckets_matrix)

    # TODO (carlosms)
    print(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run community detection analysis.')
    parser.add_argument('path', help='directory path of the parquet files')
    args = parser.parse_args()

    main(args.path)
