# make pb resolvable
import sys
sys.path.append('./pb')

from concurrent import futures
import argparse
import logging
import time

import grpc

import pb.service_pb2 as service_pb2
import pb.service_pb2_grpc as service_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Service(service_pb2_grpc.FeatureExtractorServicer):
    pass


def serve(port):
    logger = logging.getLogger('feature-extractor')

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_FeatureExtractorServicer_to_server(Service(), server)
    server.add_insecure_port('[::]:%d' % port)
    server.start()
    logger.info("server started on port %d" % port)

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Feature Extractor Service.')
    parser.add_argument("--port", type=int, default=9001,
                        help="server listen port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    serve(args.port)
