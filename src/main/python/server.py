# make pb resolvable
import sys
sys.path.append('./pb')

from concurrent import futures
import argparse
import logging
import os
import time

import grpc
from sourced.ml.extractors import IdentifiersBagExtractor, LiteralsBagExtractor, UastSeqBagExtractor

import pb.service_pb2 as service_pb2
import pb.service_pb2_grpc as service_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Service(service_pb2_grpc.FeatureExtractorServicer):
    """Feature Extractor Service"""

    def Identifiers(self, request, context):
        """Extract identifiers weighted set"""

        extractor = IdentifiersBagExtractor(
            docfreq_threshold=request.docfreqThreshold,
            split_stem=request.splitStem,
            weight=request.weight or 1)

        return self._create_response(extractor.extract(request.uast))

    def Literals(self, request, context):
        """Extract literals weighted set"""

        extractor = LiteralsBagExtractor(
            docfreq_threshold=request.docfreqThreshold,
            weight=request.weight or 1)

        return self._create_response(extractor.extract(request.uast))

    def Uast2seq(self, request, context):
        """Extract uast2seq weighted set"""

        extractor = UastSeqBagExtractor(
            docfreq_threshold=request.docfreqThreshold,
            weight=request.weight or 1)

        return self._create_response(extractor.extract(request.uast))

    def _create_response(self, f_iter):
        features = [service_pb2.Feature(
            name=f[0], weight=f[1]) for f in f_iter]

        return service_pb2.FeaturesReply(features=features)


def serve(port):
    logger = logging.getLogger('feature-extractor')

    server = _get_server(port)
    server.start()
    logger.info("server started on port %d" % port)

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


def _get_server(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_FeatureExtractorServicer_to_server(Service(), server)
    server.add_insecure_port('[::]:%d' % port)
    return server


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Feature Extractor Service.')
    parser.add_argument(
        "--port", type=int, default=9001, help="server listen port")
    args = parser.parse_args()

    # sourced-ml expects PYTHONHASHSEED != random or unset
    if os.getenv("PYTHONHASHSEED", "random") == "random":
        # The value must be between 0 and 4294967295
        # read more here: https://docs.python.org/3.3/using/cmdline.html#envvar-PYTHONHASHSEED
        raise RuntimeError("PYTHONHASHSEED must be set")

    logging.basicConfig(level=logging.INFO)
    serve(args.port)
