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
# TODO it's not best practice to bake an ssh key into the image, but
# TODO I ran into an issue doing this by mounting a volume via docker-compose.
# TODO The mounted key was owned by ec2-user so the airflow process could not read it.
# TODO Best path forward may be to generate a new ssh key for airflow and get it allow-listed
COPY /home/ec2-user/.ssh/id_rsa /home/airflow/.ssh/id_rsa
RUN chmod 600 /home/airflow/.ssh/id_rsa