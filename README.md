# ami-connect

Seamless pipelines for AMI water data, now as easy to access as turning on a tap.

## AMI Adapters

A core function of this project is to provide a set of adapters for various AMI data sources. These adapters access each AMI data source with minimal setup. They're able to extract data and transform it into our
standard format, then store that standardized data in various storage sinks, like Snowflake.

Here are the adapters in this project:

| Adapter          | Implementation Status  | Compatible Sinks  | Documentation |
|------------------|------------------------|-------------------|-------------------------------------------------|
| Beacon360        | Complete               | Snowflake         | [Link](./docs/adapters/beacon.md)               |
| Aclara           | Complete               | Snowflake         | [Link](./docs/adapters/aclara.md)               |
| Metersense       | Complete               | Snowflake         | [Link](./docs/adapters/metersense.md)           |
| Sentryx          | Complete               | Snowflake         | [Link](./docs/adapters/sentryx.md)              |
| Xylem/Sensus for Moulton Niguel | Complete.      | Snowflake         | [Link](./docs/adapters/xylem_moulton_niguel.md) |
| Subeca           | Complete               | Snowflake         | [Link](./docs/adapters/subeca.md)               |
| Neptune 360      | Complete               | Snowflake         | [Link](./docs/adapters/neptune.md)              |
| Xylem/Sensus     | Planned Q1 2026        | Snowflake         | n/a                                             |
| Harmony          | Planned                | n/a               | n/a                                             |


## Project structure

- [amiadapters](./amiadapters/) - Standalone Python library that adapts AMI data sources into our standard data format
- [amicontrol](./amicontrol/) - The control plane for our AMI data pipeline. This houses Airflow DAGs and other code to operate the pipline.
- [amideploy](./amideploy/) - IaC code to stand up an AMI Connect pipeline in the cloud. See [README](./amideploy/README.md) for instructions.
- [docs](./docs/) - End-user and maintainer documentation.
- [sql](./sql/) - Dumping ground for SQL used to manage storage sinks, e.g. Snowflake.
- [test](./test/) - Unittests for all python code in the project.

## Development

Contributions should be reviewed and approved via a Pull Request to the `main` branch. Please squash commits when you merge your PR.

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

### Using the CLI

You can use the CLI called `cli.py` to interact with AMI Connect. Turn on the virtual environment and run:

```
python cli.py --help
```

Use it to:
- Configure your production pipeline
- Run the pipeline locally

The CLI must be able to locate AWS credentials so that it can retrieve secrets and configuration. Most commands accept the `--profile` option which should match the name of
the AWS profile you're using, e.g. in your `~/.aws/credentials` file.

If you'd prefer not to specify the `--profile` option every time, you can set the `AMI_CONNECT__AWS_PROFILE` environment variable to the name of your AWS profile.

#### Configuration

Production AMI Connect pipelines are configured using:
- AWS Secrets Manager for secret credentials.
- The Snowflake database where the pipeline stores AMI data. Non-secret configuration is stored here.

To see your pipeline's configuration and secrets, run:
```
python cli.py config get --show-secrets
```

Note: AWS Secrets Manager must have your Snowflake credentials so that it can interact with the non-secret configuration in Snowflake.

You can change your configuration with the CLI using one of many commands. In general, you can use the `--help` flag or look at the code to see how they work.

#### Adding a new utility to your pipeline

One common task is to add a new source (a.k.a. utility or agency) to the system. Here's an example that adds the `my_utility` source which uses the `aclara` adapter:
```
python cli.py config add-source my_utility aclara America/Los_Angeles --sftp-host my-sftp-host --sftp-remote-data-directory ./data --sftp-local-download-directory ./output --sftp-local-known-hosts-file ./known-hosts --sinks my_snowflake
```

Then add secrets with:
```
python cli.py config update-secret my_utility --source-type aclara --sftp-user jane --sftp-password foobar
```

Each adapter type requires its own configuration and secrets. You'll need to use the correct CLI options for a given adapter. Check [./docs/adapters](./docs/adapters) for that info.

#### Running the pipeline locally

The `./cli.py` CLI will run the pipeline on your laptop. It will extract data from the sources in your local config, transform, then load the data into your configured sinks. Run with:

```
python cli.py run
```

It's common to comment out or modify lines in this script while testing.

### Deploying

Use the `deploy.sh` script to deploy new code to your AMI Connect pipeline. As of this writing, the script
bootstraps a deploy environment on the server, then remotely executes a script that builds a new Docker container with Airflow.

You'll need to tell the script the name of your environment, which should match the name of your environment in Terraform. (The script
uses Terraform outputs to configure itself).

Example deploy:
```
sh deploy.sh <my profile name>
```

You should see the script build a new Docker container and restart Airflow. Your Airflow site will be down momentarily and running DAGs will be killed. After 30s, the site should come back.

### Run Airflow application locally (rarely necessary)

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

### How to add a new Adapter for a new AMI data provider

Adapters integrate an AMI data source with our pipeline. In general, when you create one, you'll need to define
how it extracts data from the AMI data source, how it transforms that data into our generalized format, and (optionally) how it stores raw extracted data into storage sinks.

Steps:
1. In a new python file within `amiadapters`, create a new implementation of the `BaseAMIAdapter` abstract class.
2. Define an extract function that retrieves AMI data from a source and outputs it using an output controller. This function shouldn't alter the data much - its output should stay as close to the source data to reduce risk of error and to give us a historical record of the source data via our stored intermediate outputs.
3. Define a transform function that takes the data from extract and transforms it into our generic data models, then outputs it using an output controller.
4. If you're loading the raw data into a storage sink, you should define that step using something like RawSnowflakeLoader.
5. For the pipeline to run your adapter, the `AMIAdapterConfiguration.adapters()` function will need to be able to instantiate your adapter. This will require some code in `config.py`. You'll need to figure out what specific config is required by your adapter for the configuration table's `configuration_sources.sources` column, then be able to parse that block, e.g. within `AMIAdapterConfiguration.from_database()` and the CLI which is used to add/update sources.

Add any documentation that's specific to this adapter to [./docs/adapters](./docs/adapters).