from typing import Dict, List, Tuple


def get_configuration(snowflake_connection) -> Tuple[
    List[Dict],
    List[Dict],
    Dict,
    Dict,
    List[Dict],
    Dict,
]:
    """
    Given a Snowflake connection, load all raw configuration objects from the database.
    We return as dicts and lists to match the YAML config system's API.
    """
    tables = [
        "configuration_sources",
        "configuration_source_sinks",
        "configuration_sinks",
        "configuration_sink_checks",
        "configuration_task_outputs",
        "configuration_notifications",
        "configuration_backfills",
    ]
    all_config = {}
    cursor = snowflake_connection.cursor()
    for table in tables:
        all_config[table] = _fetch_table(cursor, table)

    sinks_by_source_id = {}
    for row in all_config["configuration_source_sinks"]:
        source_id = row["source_id"]
        if source_id not in sinks_by_source_id:
            sinks_by_source_id[source_id] = []
        sinks_by_source_id[source_id].append(row["sink_id"])

    sources = []
    for row in all_config["configuration_sources"]:
        source = {}
        source["type"] = row["type"]
        source["org_id"] = row["org_id"]
        source["timezone"] = row["timezone"]
        source["sinks"] = sinks_by_source_id.get(row["id"], [])
        type_specific_config = row["config"]
        match row["type"]:
            case "beacon_360" | "sentryx":
                source["use_raw_data_cache"] = type_specific_config.get(
                    "use_raw_data_cache", False
                )
            case "aclara":
                source["sftp_host"] = type_specific_config.get("sftp_host")
                source["sftp_remote_data_directory"] = type_specific_config.get(
                    "sftp_remote_data_directory"
                )
                source["sftp_local_download_directory"] = type_specific_config.get(
                    "sftp_local_download_directory"
                )
                source["sftp_local_known_hosts_file"] = type_specific_config.get(
                    "sftp_local_known_hosts_file"
                )
            case "metersense" | "xylem_moulton_niguel":
                source["ssh_tunnel_server_host"] = type_specific_config.get(
                    "ssh_tunnel_server_host"
                )
                source["ssh_tunnel_key_path"] = type_specific_config.get(
                    "ssh_tunnel_key_path"
                )
                source["database_host"] = type_specific_config.get("database_host")
                source["database_port"] = type_specific_config.get("database_port")
            case _:
                pass
        sources.append(source)

    checks_by_sink_id = {}
    for row in all_config["configuration_sink_checks"]:
        sink_id = row["sink_id"]
        if sink_id not in checks_by_sink_id:
            checks_by_sink_id[sink_id] = []
        checks_by_sink_id[sink_id].append(row["check_name"])

    sinks = []
    for row in all_config["configuration_sinks"]:
        sink = {}
        sink["id"] = row["id"]
        sink["type"] = row["type"]
        sink["checks"] = checks_by_sink_id.get(row["id"])
        sinks.append(sink)

    if len(all_config["configuration_task_outputs"]) != 1:
        raise ValueError(
            f"Expected one row for configuration_task_outputs, got {len(all_config["configuration_task_outputs"])}"
        )
    task_output = {}
    row = all_config["configuration_task_outputs"][0]
    task_output["type"] = row["type"]
    task_output["bucket"] = row["s3_bucket"]
    task_output["local_output_path"] = row["local_output_path"]

    notifications = {}
    for row in all_config["configuration_notifications"]:
        notification = {}
        event_type = row["event_type"]
        match event_type:
            case "dag_failure":
                notification["sns_arn"] = row["sns_arn"]
            case _:
                pass
        notifications[event_type] = notification

    backfills = []
    for row in all_config["configuration_backfills"]:
        backfill = {}
        backfill["org_id"] = row["org_id"]
        backfill["start_date"] = row["start_date"]
        backfill["end_date"] = row["end_date"]
        backfill["interval_days"] = row["interval_days"]
        backfill["schedule"] = row["schedule"]
        backfills.append(backfill)

    return sources, sinks, task_output, notifications, backfills


def _fetch_table(cursor, table_name):
    """Fetch all rows from a configuration table and return as list of dicts."""
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [col[0].lower() for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def update_task_output_configuration(connection, task_output_configuration: dict):
    # Validate
    if not task_output_configuration["type"]:
        raise ValueError(f"Task output configuration is missing field: type")
    if (
        task_output_configuration["type"] == "s3"
        and not task_output_configuration["s3_bucket"]
    ):
        raise ValueError(
            f"Task output configuration with type s3 is missing field: s3_bucket"
        )
    if (
        task_output_configuration["type"] == "local"
        and not task_output_configuration["local_output_path"]
    ):
        raise ValueError(
            f"Task output configuration with type local is missing field: local_output_path"
        )

    cursor = connection.cursor()
    # Remove existing row
    cursor.execute("TRUNCATE TABLE configuration_task_outputs")
    # Insert the new row
    cursor.execute(
        """
        INSERT INTO configuration_task_outputs (type, s3_bucket, local_output_path)
        VALUES (?, ?, ?)
        """,
        [
            task_output_configuration["type"],
            task_output_configuration["s3_bucket"],
            task_output_configuration["local_output_path"],
        ],
    )
