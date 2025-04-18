import json
from fabric import Connection
import psycopg2


def load_outputs(path="./outputs.json") -> dict:
    """
    Load terraform outputs file
    """
    with open(path) as f:
        return json.load(f)


def setup_ec2_server(ip_address, ssh_key_path, username="ec2-user"):
    """
    Setup EC2 server that will host the Airflow application.
    """
    conn = Connection(
        host=ip_address,
        user=username,
        connect_kwargs={
            "key_filename": ssh_key_path,
            # "options": {
            #     "AddressFamily": "inet",
            # }
        },
    )
    # conn.open_kwargs.update({
    #     "options": {
    #         "AddressFamily": "inet",  # inet = IPv4 (inet6 would be IPv6)
    #     }
    # })

    # Commands to install Python 3.12
    commands = [
        "sudo yum install python3.12",
        "curl -O https://bootstrap.pypa.io/get-pip.py",
        "python3.12 get-pip.py --user",
        "",
        "source venv/bin/activate",
    ]
    for cmd in commands:
        print(f"Running: {cmd}")
        conn.run(cmd, hide=False)
    print("✅ Python installed")

    # Create a virtual environment
    conn.run("python3.12 -m venv venv", hide=False)
    conn.run("source venv/bin/activate", hide=False)
    print("✅ Virtual environment created")

    # Install requirements inside venv
    conn.put("../requirements.txt", remote="requirements.txt")
    conn.run("pip install -r requirements.txt", hide=False)
    print("✅ Requirements installed inside virtualenv")

    conn.close()


def create_postgres(rds_endpoint, db_admin_user, db_admin_password):
    """
    Prepare the postgres database for Airflow
    """
    pass


if __name__ == "__main__":
    outputs = load_outputs()

    ec2_ip = outputs["airflow_server_ip"]["value"]
    # rds_endpoint = outputs["rds_endpoint"]["value"]

    ssh_key_path = "./airflow-key.pem"  # CHANGE THIS
    # db_admin_user = "admin"                # CHANGE THIS
    # db_admin_password = "yourpassword"      # CHANGE THIS

    print(ec2_ip, ssh_key_path)
    setup_ec2_server(ec2_ip, ssh_key_path)
    
    # create_postgres(rds_endpoint, db_admin_user, db_admin_password)

    print("🎉 Configuration completed successfully!")
