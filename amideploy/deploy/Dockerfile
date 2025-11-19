FROM apache/airflow:2.10.5-python3.12
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential python3-dev \
    && apt-get clean

USER airflow

COPY ./repo/requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

COPY ./repo/amiadapters .
COPY ./repo/amicontrol .

# Add "amiadapters" to PYTHONPATH so it'll be available to DAGs
# ENV PYTHONPATH "${PYTHONPATH}:./amiadapters"