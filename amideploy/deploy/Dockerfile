FROM apache/airflow:2.10.5-python3.12
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev \
    && apt-get clean

USER airflow

COPY ./repo/requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

COPY ./repo/amiadapters ./amiadapters
COPY ./repo/amicontrol/dags ./dags
# TODO this should handle when neptune does not exist
COPY ./neptune ./neptune
# TODO this should handle when file does not exist
COPY ./known-hosts ./known-hosts