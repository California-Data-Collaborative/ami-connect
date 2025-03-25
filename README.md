# ami-connect
Seamless pipelines for AMI water data, now as easy to access as turning on a tap.

## Development

Contributions should be reviewed and approved via a Pull Request to the `main` branch.

This is a Python 3.12 project. To set up your local python environment, create a virtual environment with:

```
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

You may need to install `rust` to get airflow to install. Follow instructions in the rust error message.

### Testing

Run unit tests with
```
python -m unittest
```

### Airflow

We use Apache Airflow to orchestrate our data pipeline. The Airflow code is in the `amicontrol` package.

With the dependencies from `requirements.txt` installed, you should be able to run Airflow locally. First,
set the AIRFLOW_HOME variable:

```
mkdir ./amicontrol/airflow && export AIRFLOW_HOME=`pwd`/amicontrol/airflow
```

If it's your first time, create the local Airflow app in `AIRFLOW_HOME` with:

```
airflow standalone
```

You'll want to modify the configuration to pick up our DAGs. In `$AIRFLOW_HOME/airflow.cfg`, change to these values:

```
dags_folder = {/your path to repo}/ami-connect/amicontrol/dags
load_examples = False
```

Before you run `airflow standalone`, set these environment variables:
```
# This allows the DAG to import code from our other packages
export PYTHONPATH="${PYTHONPATH}:./amiadapters"

# This fixes hanging HTTP requests
# https://stackoverflow.com/questions/75980623/why-is-my-airflow-hanging-up-if-i-send-a-http-request-inside-a-task
export NO_PROXY="*"
```
