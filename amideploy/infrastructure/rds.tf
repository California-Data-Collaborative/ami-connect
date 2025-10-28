resource "aws_db_instance" "ami_connect_airflow_metastore" {
  identifier                 = "ami-connect-airflow-db"
  engine                     = "postgres"
  engine_version             = "16.8"
  instance_class             = "db.t4g.micro"
  allocated_storage          = 100
  storage_type               = "gp3"
  storage_encrypted          = true
  multi_az                   = false
  backup_retention_period    = 7
  deletion_protection        = true
  enabled_cloudwatch_logs_exports = ["postgresql"]
  db_name                    = "airflow_db"
  username                   = "airflow_user"
  password                   = var.airflow_db_password
  vpc_security_group_ids     = [aws_security_group.airflow_db_sg.id]
  skip_final_snapshot        = false
  final_snapshot_identifier  = "final-snapshot-ami-connect-airflow-db"
}