# amideploy

Manage AMI Connect infrastructure and deploy new code.

Structure:
- [infrastructure](./infrastructure/): Code to create AWS resources using terraform
- [configuration](./configuration/): Code to stand up AMI Connect on AWS resources

## Prerequisites

AMI Connect is built to run on AWS infrastructure. You'll need an AWS account with credentials that permit you to create AWS resources.

We recommend you use the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and its configuration pattern to store your AWS credentials. Specifically, you should have an `~/.aws/credentials` file with an access key and secret access key.

We use `terraform` to manage infrastructure resources. [Here are installation instructions](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).

### Pick a hostname for your Airflow application

AMI Connect will build an Airflow application accessible on the public internet (but protected by a username and password). You'll want to pick a domain name for this site, then register a domain with Route 53 in your AWS account. The Route53 domain is left out of our `terraform` code because we don't want anyone to accidentally create multiple domain names. Terraform will link the hosted zone for your domain name to your Airflow webserver.

As an example: CaDC's AMI Connect deployment uses the cadc-ami-connect.com domain.

## Get started with terraform

Go to the `./amideploy/infrastructure` directory:

```
cd amideploy/infrastructure
```

We expect that you'll create a terraform workspace here. This will isolate your terraform state and
allow you to create a `*.tfvars` file for your environment.

From `amideploy/infrastructure`, run:

```
terraform init
terraform workspace new <your workspace>

mkdir environments/<your workspace>
touch environments/<your workspace>/<your workspace>.tfvars
```

`<your workspace>.tfvars` should contain your own AMI Connect infrastructure configuration. See `variables.tf` for the variables you'll specify. Here's an example `<your workspace>.tfvars` file:

```
# cat ./environments/my-workspace/my-workspace.tfvars 
aws_profile = "my-aws-profile-name-associated-with-local-credentials"
aws_region = "us-west-2"
airflow_db_password = "myairflowdbpwd"
ssh_ip_allowlist = ["my.ip.address/32"]
airflow_hostname = "my-ami-connect-domain.com"
ami_connect_s3_bucket_name = "my-s3-bucket-name"
```

The value in `aws_profile` should match the AWS profile name in your `~/.aws/credentials` file, where an AWS access key's details give access to your AWS account.

## Run terraform to create the infrastructure

Each terraform command will need to reference the `*.tfvars` file you created above. Run this to make sure
everything is working:

```

terraform plan -var-file="./environments/<your workspace>/<your workspace>.tfvars"
```

This command should exit without error. It should describe a number of resources we'll create:
- An EC2 that will host our Airflow application
- A Postgresql database for Airflow's metastore
- An Elastic IP assigned to our EC2
- An "A" record that connects your Route53 instance to that Elastic IP
- Security groups to make all of the networking work
- An S3 bucket
- An IAM role that grants permission for resources to access S3

If that looks good, create the infrastructure:

```
terraform apply -var-file="./environments/<your workspace>/<your workspace>.tfvars"
```

## Configure the infrastructure

Now that your servers are created, you'll need to configure them to run Airflow.

Access the Airflow EC2's private key, store it on your machine, and prep it for SSH:
```
terraform output airflow_server_private_key_pem > ../configuration/airflow-key.pem
chmod 600 ../configuration/airflow-key.pem
```

Put all terraform output into a file that our configuration script can reference:
```
terraform output -json > ../configuration/output.json
```

Never check these files into version control!

Go to the `./amideploy/configuration` directory:

```
cd ../configuration
```

### Copy files onto the EC2

Find the public hostname of your EC2 server. Use it to SCP files with:

```
scp -i ./airflow-key.pem ../../requirements.txt ec2-user@<public hostname>:~
```

### SSH into your EC2 server

Find the public hostname of your EC2 server. SSH into it with:

```
ssh -i ./airflow-key.pem ec2-user@<public hostname>
```

### Install and run Airflow on the EC2

On the EC2, install Python:

```
sudo yum install python3.12
curl -O https://bootstrap.pypa.io/get-pip.py
python3.12 get-pip.py --user
```

Create a virtual environment and install dependencies:
```
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Initialize the Airflow project
```
export AIRFLOW_HOME=$(pwd)
airflow info
mkdir dags
```

Gather the hostname and the airflow_user's password for your Postgres database.

There should now be an `airflow.cfg` file in the `/home/ec2-user` directory. You should modify it with the following settings, some of which already exist in the config file and some which don't:

```
[webserver]
authenticate = True
auth_backend = airflow.www.security.auth_backend.password_auth
warn_deployment_exposure = False

[core]
load_examples = False
executor = LocalExecutor

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow_user:<your airflow database password>@<your postgres hostname>:5432/airflow_db
```

Start the Airflow webserver and scheduler:
```
# Airflow will pick up our Python code here
export PYTHONPATH=/home/ec2-user

airflow db init
nohup airflow webserver &
nohup airflow scheduler &
```

Now Airflow is running. You should see a response from Airflow if you run `curl localhost:8080`.

You can create an admin user with the following:

```
airflow users create   --username admin   --firstname Admin   --lastname User   --role Admin   --email <pick an email address>   --password <pick a password>
```

### Create nginx reverse proxy

We use `nginx` to create a reverse proxy on the server. Our security groups are configured to allow
HTTP traffic on port `80`, and `nginx` will forward that traffic to Airflow.

Install and run `nginx`:
```
sudo yum install nginx -y
sudo systemctl start nginx
sudo systemctl enable nginx
```

Gather your AMI Connect domain name, then add following to `/etc/nginx/nginx.conf` inside the `http` block:
```
server {
    listen 80;
    server_name <your AMI connect domain, e.g. cadc-ami-connect.com>;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

Restart `nginx`:
```
sudo systemctl restart nginx
```

Now you should be able to access your Airflow site in your browser using the domain name you picked. Login with the username and password we created above.
