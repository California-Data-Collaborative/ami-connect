resource "tls_private_key" "airflow_server_private_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "generated_airflow_server_key" {
  key_name   = "auto-generated-key"
  public_key = tls_private_key.airflow_server_private_key.public_key_openssh
}

resource "aws_instance" "ami_connect_airflow_server" {
  ami           = "ami-087f352c165340ea1"
  instance_type = var.ami_connect_airflow_server_instance_size
  vpc_security_group_ids = [aws_security_group.airflow_server_sg.id]
  key_name      = aws_key_pair.generated_airflow_server_key.key_name

  iam_instance_profile = aws_iam_instance_profile.ami_instance_profile.name

  user_data = <<-EOF
              #!/bin/bash
              sudo yum install -y amazon-cloudwatch-agent
              cat <<EOC > /opt/aws/amazon-cloudwatch-agent/bin/config.json
              {
                "agent": {
                  "metrics_collection_interval": 60,
                  "run_as_user": "root"
                },
                "metrics": {
                  "append_dimensions": {
                    "InstanceId": "$${aws:InstanceId}"
                  },
                  "metrics_collected": {
                    "mem": {
                      "measurement": [
                        "mem_used_percent"
                      ],
                      "metrics_collection_interval": 60
                    },
                    "disk": {
                      "measurement": [
                        "used_percent"
                      ],
                      "resources": [
                        "/"
                      ],
                      "metrics_collection_interval": 60
                    },
                    "swap": {
                      "measurement": [
                        "swap_used_percent"
                      ],
                      "metrics_collection_interval": 60
                    }
                  }
                }
              }
              EOC

              /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
                -a fetch-config -m ec2 \
                -c file:/opt/aws/amazon-cloudwatch-agent/bin/config.json -s
              EOF

  tags = {
    Name = var.ami_connect_tag
  }
}

# Elastic IP
resource "aws_eip" "ami_connect_airflow_server_ip" {
  instance = aws_instance.ami_connect_airflow_server.id

  tags = {
    Name = var.ami_connect_tag
  }
}