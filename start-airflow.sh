# Utility script to start Airflow on the production server
export PYTHONPATH=$(pwd)
export AIRFLOW_HOME=$(pwd)
source venv/bin/activate
nohup airflow webserver &
nohup airflow scheduler &

