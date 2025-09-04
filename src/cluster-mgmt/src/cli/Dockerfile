ARG BASE_IMAGE=python:3.7
FROM $BASE_IMAGE

ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
ADD ./ /src/
WORKDIR /src/
ENTRYPOINT ["python3", "cs_cluster.py", "-v", "cluster", "validate", "--run-as-metric-server"]
