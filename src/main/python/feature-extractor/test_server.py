# make pb resolvable
import sys
sys.path.append('./pb')

from multiprocessing import Pool
import grpc
import json
import unittest

from google.protobuf.json_format import ParseDict as ProtoParseDict
# all grpc stuff must be imported from server and not directly from pb package
# otherwise requests will be failing with
# PicklingError: Can't pickle <class ...>: it's not the same object as ...
from server import _get_server, service_pb2, service_pb2_grpc, uast_pb


class TestServer(unittest.TestCase):
    server = None
    uast = None
    stub = None

    def setUp(self):
        with open('fixtures/server.py.proto', 'rb') as f:
            node = uast_pb.Node()
            node.ParseFromString(f.read())
            self.uast = node

        pool = Pool(processes=1)
        port = get_open_port()
        self.server = _get_server(port, 1, pool)
        self.server.start()

        channel = grpc.insecure_channel("localhost:%d" % port)
        self.stub = service_pb2_grpc.FeatureExtractorStub(channel)

    def tearDown(self):
        self.server.stop(0)

    def test_Extract(self):
        response = self.stub.Extract(
            service_pb2.ExtractRequest(
                uast=self.uast,
                identifiers=service_pb2.IdentifiersOptions(
                    docfreqThreshold=5, splitStem=False),
                literals=service_pb2.LiteralsOptions(docfreqThreshold=5),
            ))

        self.assertEqual(len(response.features), 49 + 16)
        self.assertEqual(response.features[0].name, 'i.sys')
        self.assertEqual(response.features[0].weight, 1)
        self.assertEqual(response.features[49].name, 'l.3b286224b098296c')

    def test_Identifiers(self):
        response = self.stub.Identifiers(
            service_pb2.IdentifiersRequest(
                uast=self.uast,
                options=service_pb2.IdentifiersOptions(
                    docfreqThreshold=5, splitStem=False)))

        self.assertEqual(len(response.features), 49)
        self.assertEqual(response.features[0].name, 'i.sys')
        self.assertEqual(response.features[0].weight, 1)

    def test_Literals(self):
        response = self.stub.Literals(
            service_pb2.LiteralsRequest(
                uast=self.uast,
                options=service_pb2.LiteralsOptions(docfreqThreshold=5)))

        self.assertEqual(len(response.features), 16)
        self.assertEqual(response.features[0].name, 'l.3b286224b098296c')
        self.assertEqual(response.features[0].weight, 1)

    def test_Uast2seq(self):
        response = self.stub.Uast2seq(
            service_pb2.Uast2seqRequest(
                uast=self.uast,
                options=service_pb2.Uast2seqOptions(docfreqThreshold=5)))

        self.assertEqual(len(response.features), 207)
        self.assertEqual(response.features[0].name,
                         's.alias>NoopLine>PreviousNoops>Import>Str')
        self.assertEqual(response.features[0].weight, 1)

    def test_Graphlet(self):
        response = self.stub.Graphlet(
            service_pb2.GraphletRequest(
                uast=self.uast,
                options=service_pb2.GraphletOptions(docfreqThreshold=5)))

        self.assertEqual(len(response.features), 106)
        self.assertEqual(response.features[1].name,
                         'g.Module_If_Compare_If.body')
        self.assertEqual(response.features[0].weight, 1)

    def test_with_weight(self):
        response = self.stub.Identifiers(
            service_pb2.IdentifiersRequest(
                uast=self.uast,
                options=service_pb2.IdentifiersOptions(
                    docfreqThreshold=5, splitStem=False, weight=2)))

        self.assertEqual(response.features[0].weight, 2)

        response = self.stub.Literals(
            service_pb2.LiteralsRequest(
                uast=self.uast,
                options=service_pb2.LiteralsOptions(
                    docfreqThreshold=5, weight=2)))

        self.assertEqual(response.features[0].weight, 2)

        response = self.stub.Uast2seq(
            service_pb2.Uast2seqRequest(
                uast=self.uast,
                options=service_pb2.Uast2seqOptions(
                    docfreqThreshold=5, weight=2, stride=2, seqLen=[1])))

        self.assertEqual(response.features[0].weight, 6)

        response = self.stub.Graphlet(
            service_pb2.GraphletRequest(
                uast=self.uast,
                options=service_pb2.GraphletOptions(
                    docfreqThreshold=5, weight=2)))

        self.assertEqual(response.features[0].weight, 2)


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
