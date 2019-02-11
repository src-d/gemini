import argparse

import numpy
import pyarrow as pa
import pyarrow.parquet as pq

import community_detector


def read_connected_components(filepath):
    d = pq.read_table(filepath).to_pydict()
    return dict(zip(d['cc'], d['element_ids']))


def read_buckets_matrix(filepath):
    dict = pq.read_table(filepath).to_pydict()

    id_to_buckets = dict['buckets']

    return community_detector.build_matrix(id_to_buckets)


def main(dirpath):
    connected_components = read_connected_components('%s/cc.parquet' % dirpath)
    buckets_matrix = read_buckets_matrix('%s/buckets.parquet' % dirpath)

    # The result is a list of communities. Each community is a list of element-ids
    coms = community_detector.detect_communities(connected_components,
                                                 buckets_matrix)
    com_ids = list(range(len(coms)))

    data = [pa.array(com_ids), pa.array(coms)]
    batch = pa.RecordBatch.from_arrays(data, ['community_id', 'element_ids'])

    table = pa.Table.from_batches([batch])
    pq.write_table(table, '%s/communities.parquet' % dirpath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run community detection analysis.')
    parser.add_argument('path', help='directory path of the parquet files')
    args = parser.parse_args()

    main(args.path)
