The current fixtures have been generated with python 3.6.4, and apollo commit 6b370b5f34ba9e31cf3310e70a2eff35dd978faa. In this case you will need to generate the fixtures with the new python version.

`generator.py` creates `input.npz` and `output.npz` from the `ccs.asdf` file. This file was obtained as the output of the `apollo cc` command.

To regenerate the `npz` files, besides installing `pip install -r requirements.txt` you will need to clone the apollo repository into `./apollo`.

git clone https://github.com/src-d/apollo.git
