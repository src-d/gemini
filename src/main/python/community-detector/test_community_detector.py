from collections import defaultdict
import unittest
import os

import numpy
from numpy.testing import assert_array_equal
from scipy.sparse import csr_matrix

from community_detector import detect_communities

dirname = os.path.dirname(__file__)


def build_csr_matrix(input_npz):
    return csr_matrix(
        (input_npz['id_to_buckets_data'], input_npz['id_to_buckets_indices'],
         input_npz['id_to_buckets_indptr']),
        shape=input_npz['id_to_buckets_shape'])


class TestCommunityDetector(unittest.TestCase):
    def test_detect_communities(self):
        # Read npz input
        with numpy.load("%s/fixtures/input.npz" % (dirname)) as input_npz:
            buckets = build_csr_matrix(input_npz)
            cc = input_npz['id_to_cc']
            ccs = defaultdict(list)
            for i, c in enumerate(cc):
                ccs[c].append(i)

        # Call community_detector
        communities = detect_communities(ccs, buckets)

        # Replaces CommunitiesModel().construct(communities, ccsmodel.id_to_element).save(output)
        size = sum(map(len, communities))
        data = numpy.zeros(size, dtype=numpy.uint32)
        indptr = numpy.zeros(len(communities) + 1, dtype=numpy.int64)
        pos = 0
        for i, community in enumerate(communities):
            data[pos:pos + len(community)] = community
            pos += len(community)
            indptr[i + 1] = pos

        # Read npz output
        with numpy.load("%s/fixtures/output.npz" % (dirname)) as output:
            fixture_data = output['data']
            fixture_indptr = output['indptr']

        # Assert equality
        assert_array_equal(data, fixture_data)
        assert_array_equal(indptr, fixture_indptr)


if __name__ == '__main__':
    unittest.main()
