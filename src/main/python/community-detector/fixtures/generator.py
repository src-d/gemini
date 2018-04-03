# make apollo resolvable
import sys
sys.path.append('./apollo')

from apollo.graph import ConnectedComponentsModel, CommunitiesModel, detect_communities
from apollo.__main__ import get_parser
import numpy
import csv

# Extra csv files, not used by the tests, but makes it easier to debug
DEBUG_CSV = False


def main():
    # Load an asdf file created with 'apollo cc', save it as input.npz
    ccsmodel = ConnectedComponentsModel().load("./ccs.asdf")

    numpy.savez(
        "./input.npz",
        id_to_cc=ccsmodel.id_to_cc,
        id_to_buckets_data=ccsmodel.id_to_buckets.data,
        id_to_buckets_indices=ccsmodel.id_to_buckets.indices,
        id_to_buckets_indptr=ccsmodel.id_to_buckets.indptr,
        id_to_buckets_shape=ccsmodel.id_to_buckets.shape)

    if DEBUG_CSV:
        numpy.savetxt("./csv/id_to_cc.csv", ccsmodel.id_to_cc, delimiter=",")
        numpy.savetxt(
            "./csv/id_to_buckets_data.csv",
            ccsmodel.id_to_buckets.data,
            delimiter=",")
        numpy.savetxt(
            "./csv/id_to_buckets_indices.csv",
            ccsmodel.id_to_buckets.indices,
            delimiter=",")
        numpy.savetxt(
            "./csv/id_to_buckets_indptr.csv",
            ccsmodel.id_to_buckets.indptr,
            delimiter=",")

    # Call 'apollo cmd', save output to communities.asdf
    parser = get_parser()
    args = parser.parse_args([
        'cmd', '--input', './ccs.asdf', '--output', './communities.asdf',
        '--no-spark'
    ])

    try:
        handler = args.handler
    except AttributeError:

        def print_usage(_):
            parser.print_usage()

        handler = print_usage
    handler(args)

    # Load the 'apollo cmd' output, save it as output.npz
    communities_model = CommunitiesModel().load("./communities.asdf")
    tree = communities_model._generate_tree()

    if DEBUG_CSV:
        numpy.savetxt("./csv/out_data.csv", tree["data"], delimiter=",")
        numpy.savetxt("./csv/out_indptr.csv", tree["indptr"], delimiter=",")
        with open("./csv/out_id_to_element.csv", 'w') as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(communities_model.id_to_element)

    numpy.savez("./output.npz", data=tree["data"], indptr=tree["indptr"])


if __name__ == "__main__":
    main()
