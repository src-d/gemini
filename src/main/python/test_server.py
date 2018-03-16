# make pb resolvable
import sys
sys.path.append('./pb')

import grpc
import json
import unittest

import pb.service_pb2 as service_pb2
import pb.service_pb2_grpc as service_pb2_grpc
from google.protobuf.json_format import ParseDict as ProtoParseDict
from pb.service_pb2 import gopkg_dot_in_dot_bblfsh_dot_sdk_dot_v1_dot_uast_dot_generated__pb2 as uast_pb
from server import _get_server


class TestServer(unittest.TestCase):
    port = 0
    server = None
    uast = None

    def setUp(self):
        with open('fixtures/server.py.proto', 'rb') as f:
            node = uast_pb.Node()
            node.ParseFromString(f.read())
            self.uast = node

        self.port = get_open_port()
        self.server = _get_server(self.port)
        self.server.start()

    def tearDown(self):
        self.server.stop(0)

    def test_Identifiers(self):
        channel = grpc.insecure_channel("localhost:%d" % self.port)
        stub = service_pb2_grpc.FeatureExtractorStub(channel)
        response = stub.Identifiers(service_pb2.IdentifiersRequest(
            docfreqThreshold=5, splitStem=False, uast=self.uast))

        self.assertEqual(len(response.features), 49)
        self.assertEqual(response.features[0].name, 'i.sys')
        self.assertEqual(response.features[0].weight, 1)


def get_open_port():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


if __name__ == '__main__':
    unittest.main()
