# amideploy

Manage AMI Connect infrastructure and deploy new code.

Structure:
- [infrastructure](./infrastructure/): Code to create AWS resources using terraform
- [configuration](./configuration/): Code to configure AWS resources using python scripts

## Prerequisites

AMI Connect is built to run on AWS infrastructure. You'll need an AWS account with credentials that permit you to create AWS resources.

We recommend you use the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and its configuration pattern to store your AWS credentials. Specifically, you should have an `~/.aws/credentials` file with an access key and secret access key.

We use `terraform` to manage infrastructure resources. Get started [here](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).

### Pick a hostname for your Airflow application

AMI Connect will build an Airflow application accessible on the public internet (but protected by a username and password). You'll want to pick a hostname for this site, then create a Route53
instance manually in your AWS account. This is left out of our `terraform` code because we don't want anyone to accidentally create multiple Route53 instances and hostnames.

## Get started with terraform

Go to the `./amideploy/infrastructure` directory:

```
cd amideploy/infrastructure
```

We expect that you'll create a `terraform.tfvars` file in this directory, then fill it
with your own AMI Connect infrastructure configuration. See `variables.tf` for the variables
you'll specify. Here's an example `terraform.tfvars` file:

```
# cat ./terraform.tfvars 
aws_profile = "cadc-ami-connect"
aws_region = "us-west-2"    
```

The value in `aws_profile` should match the AWS profile name in your `~/.aws/credentials` file, where an AWS access key's details
give access to your AWS account.

## Run terraform to create the infrastructure

Run `terraform` to see if everything is set up properly:

```
terraform plan
```

This command should exit without error. It should describe a number of resources we'll create:
- An EC2 that will host our Airflow application
- A Postgresql database for Airflow's metastore
- An Elastic IP assigned to our EC2
- An "A" record that connects your Route53 instance to that Elastic IP
- Security groups to make all of the networking work

If that looks good, create the infrastructure:

```
terraform apply
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








