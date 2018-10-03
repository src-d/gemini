#!/usr/bin/env python3

import os
import sys
import argparse

import bblfsh
from bblfsh import filter as filter_uast

# reference example for function extraction used in Apollo

FUNC_XPATH = "//*[@roleFunction and @roleDeclaration]"
FUNC_NAME_XPATH = "/*[@roleFunction and @roleIdentifier and @roleName] " \
                "| /*/*[@roleFunction and @roleIdentifier and @roleName]"

def extract_functions_from_uast(uast):
    """https://github.com/src-d/ml/blob/24c3d750882fceb819d82aed55115e2d9fe3c348/sourced/ml/transformers/moder.py#L80"""
    allfuncs = list(filter_uast(uast, FUNC_XPATH))
    internal = set()
    for func in allfuncs:
        if id(func) in internal:
            continue
        for sub in filter_uast(func, FUNC_XPATH):
            if sub != func:
                internal.add(id(sub))
    for f in allfuncs:
        if id(f) not in internal:
            name = "+".join(n.token for n in filter_uast(f, FUNC_NAME_XPATH))
            if name:
                yield f, name

def main():
    parser = argparse.ArgumentParser(description='Parsing a file and extracting all functions')
    parser.add_argument('file', help="path to file")
    args = parser.parse_args()

    client = bblfsh.BblfshClient("0.0.0.0:9432")
    uast = client.parse(args.file).uast

    for (fn_uast, fn_name) in extract_functions_from_uast(uast):
        print("{}_{}:{}".format(os.path.basename(args.file), fn_name, fn_uast.start_position.line))

if __name__ == '__main__':
    sys.exit(main())
