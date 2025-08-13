# ami-connect

Seamless pipelines for AMI water data, now as easy to access as turning on a tap.

## AMI Adapters

A core function of this project is to provide a set of adapters for various AMI data sources. These adapters allow you to
access each AMI data source with minimal setup. They are able to extract data from the AMI source and transform it into our
standard format. They can store that standardized data in various storage sinks, like Snowflake.

Here are the adapters in this project:

| Adapter          | Implementation Status  | Compatible Sinks  | Documentation |
|------------------|------------------------|-------------------|-------------------------------------------------|
| Beacon360        | Complete               | Snowflake         | [Link](./docs/adapters/beacon.md)               |
| Aclara           | Complete               | Snowflake         | [Link](./docs/adapters/aclara.md)               |
| Metersense       | Complete               | Snowflake         | [Link](./docs/adapters/metersense.md)           |
| Sentryx          | Complete               | Snowflake         | [Link](./docs/adapters/sentryx.md)              |
| Xylem for Moulton Niguel | Complete.      | Snowflake         | [Link](./docs/adapters/xylem_moulton_niguel.md) |
| Subeca           | Complete               | Snowflake         | [Link](./docs/adapters/subeca.md)               |
| Neptune 360      | Complete               | Snowflake         | [Link](./docs/adapters/neptune.md)              |
| Harmony          | Planned                | n/a               | n/a                                             |


## Project structure

- [amiadapters](./amiadapters/) - Standalone Python library that adapts AMI data sources into our standard data format
- [amicontrol](./amicontrol/) - The control plane for our AMI data pipeline. This houses Airflow DAGs and other code to operate the pipline.
- [amideploy](./amideploy/) - IaC code to stand up an AMI Connect pipeline in the cloud. See [README](./amideploy/README.md) for instructions.
- [docs](./docs/) - End-user and maintainer documentation.
- [sql](./sql/) - Dumping ground for SQL used to manage storage sinks, e.g. Snowflake.
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

### Configuration

We use two YAML files to configure the pipeline:
- `config.yaml`, which tells the pipeline which AMI data sources should be queried during a run and which storage sinks should receive the data.
- `secrets.yaml`, which houses all secrets.

Copy our example files to get started:
```
cp ./config.yaml.example ./config.yaml
cp ./secrets.yaml.example ./secrets.yaml
```

### Deploy

Use the `deploy.sh` script to deploy new code to your AMI Connect pipeline. As of this writing, the script
simply copies new code to the Airflow server and updates python dependencies.

You'll need to tell the script the hostname of your Airflow server. You can set the `AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME` environment variable to your hostname.

The script assumes you've stored a key pair at `./amideploy/configuration/airflow-key.pem`.

For configuration, it expects a `config.prod.yaml` and `secrets.prod.yaml` file. These will be used to configure
your production pipeline.

Example deploy:
```
export AMI_CONNECT__AIRFLOW_SERVER_HOSTNAME=<my EC2 hostname> && sh deploy.sh
```

### Run Airflow application locally

We use Apache Airflow to orchestrate our data pipeline. The Airflow code is in the `amicontrol` package.

With the dependencies from `requirements.txt` installed, you should be able to run Airflow locally. First,
set the AIRFLOW_HOME variable and your PYTHONPATH:

```
mkdir ./amicontrol/airflow
export AIRFLOW_HOME=`pwd`/amicontrol/airflow
export PYTHONPATH="${PYTHONPATH}:./amiadapters"
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
# This fixes hanging HTTP requests
# See: https://stackoverflow.com/questions/75980623/why-is-my-airflow-hanging-up-if-i-send-a-http-request-inside-a-task
export NO_PROXY="*"
```

Re-run the command to start the application:
```
airflow standalone
```
Watch for the `admin` user and its password in `stdout`. Use those credentials to login. You should see our DAGs!

## How to add a new Adapter

Adapters integrate an AMI data source with our pipeline. In general, when you create one, you'll need to define
how it extracts data from the AMI data source, how it transforms that data into our generalized format, and (optionally) how it stores raw extracted data into storage sinks.

Steps:
1. In a new python file within `amiadapters`, create a new implementation of the `BaseAMIAdapter` abstract class.
2. Define an extract function that retrieves AMI data from a source and outputs it using an output controller. This function shouldn't alter the data much - its output should stay as close to the source data to reduce risk of error and to give us a historical record of the source data via our stored intermediate outputs.
3. Define a transform function that takes the data from extract and transforms it into our generic data models, then outputs it using an output controller.
4. If you're loading the raw data into a storage sink, you should define that step using something like RawSnowflakeLoader.
5. For the pipeline to run your adapter, the `AMIAdapterConfiguration.adapters()` function will need to be able to instantiate your adapter. This will require some code in `config.py`. You'll need to figure out what a block in the `sources` section of the config looks like for your adapter, then be able to parse that block, e.g. within `AMIAdapterConfiguration.from_yaml()`.

Add any documentation that's specific to this adapter to [./docs/adapters](./docs/adapters).