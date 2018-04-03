import unittest
import numpy
from numpy.testing import assert_array_equal
from scipy.sparse import csr_matrix
from community_detector import detect_communities


def build_csr_matrix(input):
    return csr_matrix(
        (input['id_to_buckets_data'], input['id_to_buckets_indices'],
         input['id_to_buckets_indptr']),
        shape=input['id_to_buckets_shape'])


class TestServer(unittest.TestCase):
    def test_detect_communities(self):
        # Read npz input
        with numpy.load('fixtures/input.npz') as input:
            buckets = build_csr_matrix(input)
            cc = input['id_to_cc']

        # Call community_detector
        result = detect_communities(cc, buckets)

        # Read npz output
        with numpy.load('fixtures/output.npz') as output:
            fixture_data = output['data']
            fixture_indptr = output['indptr']

        # Assert equality
        assert_array_equal(result['data'], fixture_data)
        assert_array_equal(result['indptr'], fixture_indptr)


if __name__ == '__main__':
    unittest.main()
