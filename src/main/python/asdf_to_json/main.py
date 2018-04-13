"""
Converter for necessary models from .asdf format produced by apollo to json

Usage:
python3 main.py -m params /path/to/params_model.asdf
produces /path/to/params_model.json

python3 main.py -m docfreq /path/to/docfreq_model.asdf
produces /path/to/docfreq_model.json

"""

import json
import os.path
import sys
import argparse

from modelforge.model import Model
from modelforge.models import register_model
from sourced.ml.models import OrderedDocumentFrequencies


# copy-paste from apollo to avoid dependency:
# https://github.com/src-d/apollo/blob/3d3625d94eb844ed78892b1c246a1770a6ccf18b/apollo/hasher.py#L64
@register_model
class WeightedMinHashParameters(Model):
    """
    The randomly generated parameters of the Weighted MinHash-er.
    """
    NAME = "wmhparams"

    def construct(self, rs, ln_cs, betas):
        self.rs = rs
        self.ln_cs = ln_cs
        self.betas = betas
        # do not remove - this loads the arrays from disk
        rs[0] + ln_cs[0] + betas[0]
        return self

    def _load_tree(self, tree):
        self.construct(rs=tree["rs"], ln_cs=tree["ln_cs"], betas=tree["betas"])

    def dump(self):
        return """Shape: %s""" % self.rs.shape

    def _generate_tree(self):
        return {"rs": self.rs, "ln_cs": self.ln_cs, "betas": self.betas}


models = {
    'docfreq': {
        'class': OrderedDocumentFrequencies,
        'transform': lambda x: {
            'docs': x.docs,
            'tokens': x.tokens(),
            'df': {k: float(v) for k, v in x._df.items()},
        }
    },
    'params': {
        'class': WeightedMinHashParameters,
        'transform': lambda x: {
            'rs': x.rs.tolist(),
            'ln_cs': x.ln_cs.tolist(),
            'betas': x.betas.tolist(),
        },
    }
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("asdf", help="path to .asdf file")
    parser.add_argument(
        "-m",
        "--model",
        required=True,
        help="src-d.ml model",
        choices=["docfreq", "params"])

    args = parser.parse_args()

    dirname = os.path.dirname(args.asdf)
    filename = os.path.basename(args.asdf)
    name = os.path.splitext(filename)[0]
    json_path = os.path.join(dirname, "%s.json" % name)

    m = models[args.model]
    model = m['class']().load(args.asdf)
    j = m['transform'](model)

    with open(json_path, 'w') as outfile:
        json.dump(j, outfile)
