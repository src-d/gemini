# make pb resolvable
import sys
sys.path.append('./pb')

from concurrent import futures
import argparse
import logging
import os
import time

import grpc
from sourced.ml.extractors import IdentifiersBagExtractor, LiteralsBagExtractor, \
    UastSeqBagExtractor, GraphletBagExtractor

import pb.service_pb2 as service_pb2
import pb.service_pb2_grpc as service_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Service(service_pb2_grpc.FeatureExtractorServicer):
    """Feature Extractor Service"""

    extractors_names = ["identifiers", "literals", "uast2seq", "graphlet"]

    def Extract(self, request, context):
        """ Extract features using multiple extrators """

        extractors = []

        for name in self.extractors_names:
            if request.HasField(name):
                options = getattr(request, name, None)
                if options is None:
                    continue
                constructor = getattr(self, "_%s_extractor" % name)
                extractors.append(constructor(options))

        features = []

        for ex in extractors:
            features.extend(_features_iter_to_list(ex.extract(request.uast)))

        return service_pb2.FeaturesReply(features=features)

    def Identifiers(self, request, context):
        """Extract identifiers weighted set"""

        it = self._identifiers_extractor(request.options).extract(request.uast)
        return service_pb2.FeaturesReply(features=_features_iter_to_list(it))

    def Literals(self, request, context):
        """Extract literals weighted set"""

        it = self._literals_extractor(request.options).extract(request.uast)
        return service_pb2.FeaturesReply(features=_features_iter_to_list(it))

    def Uast2seq(self, request, context):
        """Extract uast2seq weighted set"""

        it = self._uast2seq_extractor(request.options).extract(request.uast)
        return service_pb2.FeaturesReply(features=_features_iter_to_list(it))

    def Graphlet(self, request, context):
        """Extract graphlet weighted set"""

        it = self._graphlet_extractor(request.options).extract(request.uast)
        return service_pb2.FeaturesReply(features=_features_iter_to_list(it))

    def _identifiers_extractor(self, options):
        return IdentifiersBagExtractor(
            docfreq_threshold=options.docfreqThreshold,
            split_stem=options.splitStem,
            weight=options.weight or 1)

    def _literals_extractor(self, options):
        return LiteralsBagExtractor(
            docfreq_threshold=options.docfreqThreshold,
            weight=options.weight or 1)

    def _uast2seq_extractor(self, options):
        seq_len = list(options.seqLen) if options.seqLen else None

        return UastSeqBagExtractor(
            docfreq_threshold=options.docfreqThreshold,
            weight=options.weight or 1,
            stride=options.stride or 1,
            seq_len=seq_len or 5)

    def _graphlet_extractor(self, options):
        return GraphletBagExtractor(
            docfreq_threshold=options.docfreqThreshold,
            weight=options.weight or 1)


def _features_iter_to_list(f_iter):
    return [service_pb2.Feature(name=f[0], weight=f[1]) for f in f_iter]


def serve(port, workers):
    logger = logging.getLogger('feature-extractor')

    server = _get_server(port, workers)
    server.start()
    logger.info("server started on port %d" % port)

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


def _get_server(port, workers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    service_pb2_grpc.add_FeatureExtractorServicer_to_server(Service(), server)
    server.add_insecure_port('[::]:%d' % port)
    return server


if __name__ == '__main__':
    port = int(os.getenv('FEATURE_EXT_PORT', "9001"))
    workers = int(os.getenv('FEATURE_EXT_WORKERS', "10"))

    parser = argparse.ArgumentParser(description='Feature Extractor Service.')
    parser.add_argument(
        "--port", type=int, default=port, help="server listen port")
    parser.add_argument(
        "--workers",
        type=int,
        default=workers,
        help="number of service workers")
    args = parser.parse_args()

    # sourced-ml expects PYTHONHASHSEED != random or unset
    if os.getenv("PYTHONHASHSEED", "random") == "random":
        # The value must be between 0 and 4294967295
        # read more here: https://docs.python.org/3.3/using/cmdline.html#envvar-PYTHONHASHSEED
        raise RuntimeError("PYTHONHASHSEED must be set")

    logging.basicConfig(level=logging.INFO)
    serve(args.port, args.workers)
