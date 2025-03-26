# ami-connect

Seamless pipelines for AMI water data, now as easy to access as turning on a tap.

## Project structure

- [amiadapters](./amiadapters/) - Standalone Python library that adapts AMI data sources into our standard data format
- [amicontrol](./amicontrol/) - The control plane for our AMI data pipeline. This houses Airflow DAGs and other code to operate the pipline.
- [test](./test/) - Unittests for all python code in the project.

## Development

Contributions should be reviewed and approved via a Pull Request to the `main` branch.

This is a Python 3.12 project. To set up your local python environment, create a virtual environment with:

```
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

You may need to install `rust` to get airflow to install. Follow instructions in the rust error message.

### Formatting

We use `black` to format Python files. Before making a commit, format files with:

```
black .
```

### Testing

Run unit tests from the project's root directory with
```
python -m unittest
```

### Run Airflow application

We use Apache Airflow to orchestrate our data pipeline. The Airflow code is in the `amicontrol` package.

With the dependencies from `requirements.txt` installed, you should be able to run Airflow locally. First,
set the AIRFLOW_HOME variable:

```
mkdir ./amicontrol/airflow
export AIRFLOW_HOME=`pwd`/amicontrol/airflow
```

If it's your first time running the application on your machine, initialize the local Airflow app in `AIRFLOW_HOME` with:

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
# This allows the DAG to import code from this project's other Python packages
export PYTHONPATH="${PYTHONPATH}:./amiadapters"

# This fixes hanging HTTP requests
# See: https://stackoverflow.com/questions/75980623/why-is-my-airflow-hanging-up-if-i-send-a-http-request-inside-a-task
export NO_PROXY="*"
```

Re-run the command to start the application:
```
airflow standalone
```
Watch for the `admin` user and its password in `stdout`. Use those credentials to login. You should see our DAGs!