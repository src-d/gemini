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
    server = None
    uast = None
    stub = None

    def setUp(self):
        with open('fixtures/server.py.proto', 'rb') as f:
            node = uast_pb.Node()
            node.ParseFromString(f.read())
            self.uast = node

        port = get_open_port()
        self.server = _get_server(port)
        self.server.start()

        channel = grpc.insecure_channel("localhost:%d" % port)
        self.stub = service_pb2_grpc.FeatureExtractorStub(channel)

    def tearDown(self):
        self.server.stop(0)

    def test_Identifiers(self):
        response = self.stub.Identifiers(service_pb2.IdentifiersRequest(
            docfreqThreshold=5, splitStem=False, uast=self.uast))

        self.assertEqual(len(response.features), 49)
        self.assertEqual(response.features[0].name, 'i.sys')
        self.assertEqual(response.features[0].weight, 1)

    def test_Literals(self):
        response = self.stub.Literals(service_pb2.LiteralsRequest(
            docfreqThreshold=5, uast=self.uast))

        self.assertEqual(len(response.features), 16)
        self.assertEqual(response.features[0].name, 'l.3b286224b098296c')
        self.assertEqual(response.features[0].weight, 1)

    def test_Uast2seq(self):
        response = self.stub.Uast2seq(service_pb2.Uast2seqRequest(
            docfreqThreshold=5, uast=self.uast))

        self.assertEqual(len(response.features), 207)
        self.assertEqual(
            response.features[0].name, 's.alias>NoopLine>PreviousNoops>Import>Str')
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
