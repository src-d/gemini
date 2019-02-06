# make pb resolvable
import sys
sys.path.append('./pb')

from concurrent import futures
from multiprocessing import Pool
import argparse
import logging
import os
import signal
import time

import grpc
from sourced.ml.extractors import IdentifiersBagExtractor, LiteralsBagExtractor, \
    UastSeqBagExtractor, GraphletBagExtractor

import pb.service_pb2 as service_pb2
import pb.service_pb2_grpc as service_pb2_grpc
# isn't used but re-exported so it can be used in tests
from pb.service_pb2 import gopkg_dot_in_dot_bblfsh_dot_sdk_dot_v1_dot_uast_dot_generated__pb2 as uast_pb

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


# keep extractors out of the Service class to be able to pickle them
# return list instead of iterator for pickle also


def _identifiers_extractor(uast, options):
    return list(IdentifiersBagExtractor(
        docfreq_threshold=options.docfreqThreshold,
        split_stem=options.splitStem,
        weight=options.weight or 1).extract(uast))


def _literals_extractor(uast, options):
    return list(LiteralsBagExtractor(
        docfreq_threshold=options.docfreqThreshold,
        weight=options.weight or 1).extract(uast))


def _uast2seq_extractor(uast, options):
    seq_len = list(options.seqLen) if options.seqLen else None

    return list(UastSeqBagExtractor(
        docfreq_threshold=options.docfreqThreshold,
        weight=options.weight or 1,
        stride=options.stride or 1,
        seq_len=seq_len or 5).extract(uast))


def _graphlet_extractor(uast, options):
    return list(GraphletBagExtractor(
        docfreq_threshold=options.docfreqThreshold,
        weight=options.weight or 1).extract(uast))


def _features_from_iter(f_iter):
    return [service_pb2.Feature(name=f[0], weight=f[1]) for f in f_iter]


class Service(service_pb2_grpc.FeatureExtractorServicer):
    """Feature Extractor Service"""

    pool = None
    extractors = {
        "identifiers": _identifiers_extractor,
        "literals": _literals_extractor,
        "uast2seq": _uast2seq_extractor,
        "graphlet": _graphlet_extractor,
    }

    def __init__(self, pool):
        super(Service, self)
        self.pool = pool

    def Extract(self, request, context):
        """ Extract features using multiple extrators """

        results = []

        for name in self.extractors:
            if request.HasField(name):
                options = getattr(request, name, None)
                if options is None:
                    continue

                result = self.pool.apply_async(self.extractors[name],
                                               (request.uast, options))
                results.append(result)

        features = []

        for result in results:
            features.extend(_features_from_iter(result.get()))

        return service_pb2.FeaturesReply(features=features)

    def Identifiers(self, request, context):
        """Extract identifiers weighted set"""

        it = self.pool.apply(_identifiers_extractor,
                             (request.uast, request.options))
        return service_pb2.FeaturesReply(features=_features_from_iter(it))

    def Literals(self, request, context):
        """Extract literals weighted set"""

        it = self.pool.apply(_literals_extractor,
                             (request.uast, request.options))
        return service_pb2.FeaturesReply(features=_features_from_iter(it))

    def Uast2seq(self, request, context):
        """Extract uast2seq weighted set"""

        it = self.pool.apply(_uast2seq_extractor,
                             (request.uast, request.options))
        return service_pb2.FeaturesReply(features=_features_from_iter(it))

    def Graphlet(self, request, context):
        """Extract graphlet weighted set"""

        it = self.pool.apply(_graphlet_extractor,
                             (request.uast, request.options))
        return service_pb2.FeaturesReply(features=_features_from_iter(it))


def worker_init():
    """ ignore SIGINT (Ctrl-C) event inside workers.
        Read more here:
        https://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def serve(port, workers):
    logger = logging.getLogger('feature-extractor')

    # processes=None uses os.cpu_count() as a value
    pool = Pool(processes=None, initializer=worker_init)

    server = _get_server(port, workers, pool)
    server.start()
    logger.info("server started on port %d" % port)

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pool.terminate()
        server.stop(0)


def _get_server(port, workers, pool):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    service_pb2_grpc.add_FeatureExtractorServicer_to_server(
        Service(pool), server)
    server.add_insecure_port('[::]:%d' % port)
    return server


if __name__ == '__main__':
    port = int(os.getenv('FEATURE_EXT_PORT', "9001"))
    workers = int(os.getenv('FEATURE_EXT_WORKERS', "100"))

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
