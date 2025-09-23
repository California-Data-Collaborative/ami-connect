import logging
import json
from typing import Dict, List, Tuple

import pytz

logger = logging.getLogger(__name__)


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
        type_specific_config = json.loads(row["config"]) if row["config"] else {}
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


def update_source_configuration(connection, source_configuration: dict):
    # Validate
    if not source_configuration.get("org_id"):
        raise ValueError(f"Source configuration is missing field: org_id")
    org_id = source_configuration["org_id"].lower()

    cursor = connection.cursor()
    result = cursor.execute(
        """
        SELECT s.type, s.org_id, s.timezone, s.config
        FROM configuration_sources s
        WHERE s.org_id = ?
    """,
        (org_id,),
    ).fetchall()
    if len(result) > 1:
        raise Exception(f"Invalid state: more than one source with org_id {org_id}")

    if not result:
        # Insert new source
        if not source_configuration.get("type"):
            raise ValueError(f"Source configuration is missing field: type")
        if not source_configuration.get("timezone"):
            raise ValueError(f"Source configuration is missing field: timezone")
        if not source_configuration["timezone"] in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {source_configuration["timezone"]}")
        source_type = source_configuration["type"].lower()
        # Type-specific configs
        match source_type:
            case "aclara":
                config = {}
                for field in [
                    "sftp_host",
                    "sftp_remote_data_directory",
                    "sftp_local_download_directory",
                    "sftp_local_known_hosts_file",
                ]:
                    if not source_configuration.get(field):
                        raise ValueError(
                            f"Source configuration is missing field: {field}"
                        )
                    config[field] = source_configuration[field]
            case "beacon_360" | "sentryx":
                config = {
                    "use_raw_data_cache": source_configuration.get(
                        "use_raw_data_cache", False
                    )
                }
            case "metersense" | "xylem_moulton_niguel":
                config = {}
                for field in [
                    "ssh_tunnel_server_host",
                    "ssh_tunnel_key_path",
                    "database_host",
                    "database_port",
                ]:
                    if not source_configuration.get(field):
                        raise ValueError(
                            f"Source configuration is missing field: {field}"
                        )
                    config[field] = source_configuration[field]
            case "subeca":
                config = {}
            case _:
                config = {}
        logger.info(
            f"Adding new source with type={type} org_id={org_id} timezone={source_configuration["timezone"]} config={config}"
        )
        cursor.execute(
            """
            INSERT INTO configuration_sources (type, org_id, timezone, config)
            SELECT ?, ?, ?, PARSE_JSON(?);
        """,
            (source_type, org_id, source_configuration["timezone"], json.dumps(config)),
        )
    else:
        existing_source = result[0][0]
        import pdb

        pdb.set_trace()
        # Update
        cursor.execute(
            """
            MERGE INTO configuration_sinks AS target
            USING (
                SELECT ? AS id, ? AS type
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET
                    type = source.type
            WHEN NOT MATCHED THEN
                INSERT (id, type)
                VALUES (source.id, source.type);

        """,
            (sink_id, sink_type),
        )


def remove_source_configuration(connection, org_id: str):
    pass


def update_sink_configuration(connection, sink_configuration: dict):
    # Validate
    if not sink_configuration.get("id"):
        raise ValueError(f"Sink configuration is missing field: id")
    if not sink_configuration.get("type"):
        raise ValueError(f"Sink configuration is missing field: type")

    sink_id = sink_configuration["id"].lower()
    sink_type = sink_configuration["type"].lower()
    if not sink_type in ("snowflake",):
        raise ValueError(f"Unrecognized sink type: {sink_type}")

    cursor = connection.cursor()
    cursor.execute(
        """
        MERGE INTO configuration_sinks AS target
        USING (
            SELECT ? AS id, ? AS type
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                type = source.type
        WHEN NOT MATCHED THEN
            INSERT (id, type)
            VALUES (source.id, source.type);

    """,
        (sink_id, sink_type),
    )


def remove_sink_configuration(connection, id: str):
    # Validate
    if not id:
        raise ValueError(f"Missing field: id")
    sink_id = id.lower()
    cursor = connection.cursor()
    result = cursor.execute(
        """
        SELECT s.org_id
        FROM configuration_source_sinks ss
        JOIN configuration_sources s ON ss.source_id = s.id
        WHERE ss.sink_id = ?
    """,
        (sink_id,),
    ).fetchall()
    if result:

        connected_sources = ", ".join(r[0] for r in result)
        raise ValueError(
            f"Cannot remove sink {id} because it's connected to sources: {connected_sources}"
        )
    # Remove
    cursor.execute(
        """
        DELETE FROM configuration_sinks
        WHERE id = ?
    """,
        (sink_id,),
    )


def update_task_output_configuration(connection, task_output_configuration: dict):
    # Validate
    if not task_output_configuration.get("type"):
        raise ValueError(f"Task output configuration is missing field: type")
    if (
        task_output_configuration.get("type") == "s3"
        and not task_output_configuration["s3_bucket"]
    ):
        raise ValueError(
            f"Task output configuration with type s3 is missing field: s3_bucket"
        )
    if (
        task_output_configuration.get("type") == "local"
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
