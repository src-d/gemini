from itertools import chain
import logging

from igraph import Graph
import numpy
from scipy.sparse import csr_matrix


def build_matrix(id_to_buckets):
    """Builds a CSR matrix from a list of lists of buckets

    Args:
        id_to_buckets: list of lists of elementid, buckets.

    Returns:
        A scipy.sparse.csr_matrix with the same contents
    """

    if len(id_to_buckets) == 0:
        return csr_matrix((0, 0), dtype=numpy.uint8)

    max_el_id = max((item[0] for item in id_to_buckets))
    data = numpy.ones(
        sum((len(item[1]) for item in id_to_buckets)), dtype=numpy.uint8)
    indices = numpy.zeros(len(data), dtype=numpy.uint32)
    indptr = numpy.zeros(max_el_id + 2, dtype=numpy.uint32)
    pos = 0
    for el_id, bucket in id_to_buckets:
        indices[pos:(pos + len(bucket))] = bucket
        pos += len(bucket)
        indptr[el_id + 1:] = pos
    return csr_matrix((data, indices, indptr))


def detect_communities(ccs,
                       buckets_matrix,
                       edges="linear",
                       algorithm="walktrap",
                       algorithm_params={}):
    """Runs Community Detection analysis on the given connected components.

    Based largely on the Apollo detect_communities() code from
    https://github.com/src-d/apollo/blob/6b370b5f34ba9e31cf3310e70a2eff35dd978faa/apollo/graph.py#L191

    Args:
        ccs: dict with the connected components. Index is the connected component, the
            value is the list of element ids
        buckets_matrix: scipy.sparse.csr_matrix with the buckets. One row for
            each element, with a column for each bucket. If the element is in a
            bucket, the corresponding row,column (element id, bucket id) is 1,
            0 otherwise
        edges: The method to generate the graph's edges:
            - linear: linear and fast, but may not fit some of the CD
                algorithms, or all to all within a bucket
            - quadratic: slow, but surely fits all the algorithms.
        algorithm: The community detection algorithm to apply.
        algorithm_params: Parameters for the algorithm (**kwargs, JSON format).

    Returns:
        A list of communities. Each community is a list of element-ids
    """

    if edges != "linear" and edges != "quadratic":
        raise ValueError(
            "edges arg: expected one of 'linear', 'quadratic', received '%s'" %
            (edges))

    log = logging.getLogger("community-detector")
    log.debug("Building the connected components")

    buckindices = buckets_matrix.indices
    buckindptr = buckets_matrix.indptr
    total_nvertices = buckets_matrix.shape[0]
    linear = (edges == "linear")
    graphs = []
    communities = []

    if not linear:
        log.debug("Transposing the matrix")
        buckmat_csc = buckets_matrix.T.tocsr()

    fat_ccs = []

    for vertices in ccs.values():
        if len(vertices) == 1:
            continue
        if len(vertices) == 2:
            communities.append(vertices)
            continue
        fat_ccs.append(vertices)

    log.debug("Building %d graphs", len(fat_ccs))
    bucket_weights = buckets_matrix.sum(axis=0)

    for vertices in fat_ccs:
        if linear:
            edges = []
            weights = []
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
                buckverts = buckmat_csc.indices[
                    buckmat_csc.indptr[bucket]:buckmat_csc.indptr[bucket + 1]]
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

    log.debug("Launching the community detection")
    detector = CommunityDetector(algorithm=algorithm, config=algorithm_params)

    communities.extend(chain.from_iterable((detector(g) for g in graphs)))

    if len(communities) > 0:
        log.debug("Overall communities: %d", len(communities))
        log.debug("Average community size: %.1f",
                  numpy.mean([len(c) for c in communities]))
        log.debug("Median community size: %.1f",
                  numpy.median([len(c) for c in communities]))
        log.debug("Max community size: %d", max(map(len, communities)))

    return communities


class CommunityDetector:
    """Class to initialize the graph community algorithm and its arguments

    Copied from the Apollo code
    https://github.com/src-d/apollo/blob/6b370b5f34ba9e31cf3310e70a2eff35dd978faa/apollo/graph.py#L267
    """

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
        kwargs.update(self.config)
        result = action(**kwargs)

        if hasattr(result, "as_clustering"):
            result = result.as_clustering()

        output = [[] for _ in range(len(result.sizes()))]
        for i, memb in enumerate(result.membership):
            output[memb].append(int(graph.vs[i]["name"]))

        return output
