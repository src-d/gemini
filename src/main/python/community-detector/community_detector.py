from collections import defaultdict
from itertools import chain
import logging
from igraph import Graph
import numpy


def detect_communities(cc,
                       buckets_matrix,
                       edges="linear",
                       algorithm="walktrap",
                       algorithm_params={}):
    """
    Runs Community Detection analysis on the given connected components.
    :param cc: numpy.ndarray with the connected components
    :param buckets_matrix: scipy.sparse.csr_matrix with the buckets
    :param edges: The method to generate the graph's edges:
        - linear: linear and fast, but may not fit some of the CD algorithms, or all to all within a bucket
        - quadratic: slow, but surely fits all the algorithms.
    :param algorithm: The community detection algorithm to apply.
    :param algorithm_params: Parameters for the algorithm (**kwargs, JSON format).
    :return: JSON with data, indptr
    """

    log = logging.getLogger("community-detector")
    log.info("Building the connected components")

    ccs = defaultdict(list)

    for i, c in enumerate(cc):
        ccs[c].append(i)

    buckindices = buckets_matrix.indices
    buckindptr = buckets_matrix.indptr
    total_nvertices = buckets_matrix.shape[0]
    linear = edges in ("linear", "1")
    graphs = []
    communities = []

    if not linear:
        log.info("Transposing the matrix")
        buckmat_csc = buckets_matrix.T.tocsr()

    fat_ccs = []

    for vertices in ccs.values():
        if len(vertices) == 1:
            continue
        if len(vertices) == 2:
            communities.append(vertices)
            continue
        fat_ccs.append(vertices)

    log.info("Building %d graphs", len(fat_ccs))

    for vertices in fat_ccs:
        if linear:
            edges = []
            weights = []
            bucket_weights = buckets_matrix.sum(axis=0)
            buckets = set()
            for i in vertices:
                for j in range(buckindptr[i], buckindptr[i + 1]):
                    bucket = buckindices[j]
                    weights.append(bucket_weights[0, bucket])
                    bucket += total_nvertices
                    buckets.add(bucket)
                    edges.append((str(i), str(bucket)))
        else:
            edges = set()
            weights = None
            buckets = set()
            for i in vertices:
                for j in range(buckindptr[i], buckindptr[i + 1]):
                    buckets.add(buckindices[j])
            for bucket in buckets:
                buckverts = \
                    buckmat_csc.indices[buckmat_csc.indptr[bucket]:buckmat_csc.indptr[bucket + 1]]
                for i, x in enumerate(buckverts):
                    for y in buckverts:
                        if x < y:
                            edges.add((str(x), str(y)))
            buckets.clear()
            edges = list(edges)

        graph = Graph(directed=False)
        graph.add_vertices(list(map(str, vertices + list(buckets))))
        graph.add_edges(edges)
        graph.edge_weights = weights
        graphs.append(graph)

    log.info("Launching the community detection")
    detector = CommunityDetector(algorithm=algorithm, config=algorithm_params)

    communities.extend(chain.from_iterable((detector(g) for g in graphs)))

    log.info("Overall communities: %d", len(communities))
    log.info("Average community size: %.1f",
             numpy.mean([len(c) for c in communities]))
    log.info("Median community size: %.1f",
             numpy.median([len(c) for c in communities]))
    log.info("Max community size: %d", max(map(len, communities)))

    # Replaces CommunitiesModel().construct(communities, ccsmodel.id_to_element).save(output)
    size = sum(map(len, communities))
    data = numpy.zeros(size, dtype=numpy.uint32)
    indptr = numpy.zeros(len(communities) + 1, dtype=numpy.int64)
    pos = 0
    for i, community in enumerate(communities):
        data[pos:pos + len(community)] = community
        pos += len(community)
        indptr[i + 1] = pos

    return {"data": data, "indptr": indptr}


class CommunityDetector:
    def __init__(self, algorithm, config):
        self.algorithm = algorithm
        self.config = config

    def __call__(self, graph):
        action = getattr(graph, "community_" + self.algorithm)
        if self.algorithm == "infomap":
            kwargs = {"edge_weights": graph.edge_weights}
        elif self.algorithm == "leading_eigenvector_naive":
            kwargs = {}
        else:
            kwargs = {"weights": graph.edge_weights}
        if self.algorithm == "edge_betweenness":
            kwargs["directed"] = False
        result = action(**kwargs, **self.config)

        if hasattr(result, "as_clustering"):
            result = result.as_clustering()

        output = [[] for _ in range(len(result.sizes()))]
        for i, memb in enumerate(result.membership):
            output[memb].append(int(graph.vs[i]["name"]))

        return output
