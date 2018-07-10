FROM python:3.6

WORKDIR /usr/src/app

COPY src/main/python/feature-extractor/requirements.txt ./

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends libsnappy-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get remove -y libsnappy-dev && \
    apt-get remove -y .*-doc .*-man >/dev/null && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY src/main/python/feature-extractor .

CMD [ "python", "./server.py" ]
