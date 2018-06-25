# Community detector

Uses IGraph library to detect a communities inside similar code graph.

## Install dependencies

Suggested way is to use a fresh virtualenv:
```
# native dependencies
apt-get install build-essential cmake pkg-config libxml2-dev zlib1g-dev virtualenv

# virtualenv
virtualenv --no-site-packages -p python3 .venv
source .venv/bin/activate
pip install --upgrade pip

pip3 install -r src/main/python/community-detector/requirements.txt
```