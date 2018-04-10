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

        # Call community_detector
        result = detect_communities(cc.tolist(), buckets)

        # Read npz output
        with numpy.load("%s/fixtures/output.npz" % (dirname)) as output:
            fixture_data = output['data']
            fixture_indptr = output['indptr']

        # Assert equality
        assert_array_equal(result['data'], fixture_data)
        assert_array_equal(result['indptr'], fixture_indptr)


if __name__ == '__main__':
    unittest.main()
