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


def add_source_configuration(connection, source_configuration: dict):
    # Validate
    if not source_configuration.get("org_id"):
        raise ValueError(f"Source configuration is missing field: org_id")
    org_id = source_configuration["org_id"].lower()

    cursor = connection.cursor()
    existing = _get_source_by_org_id(cursor, org_id)
    if len(existing) != 0:
        raise Exception(f"Source with org_id {org_id} already exists")

    # Insert new source
    if not source_configuration.get("type"):
        raise ValueError(f"Source configuration is missing field: type")
    if not source_configuration.get("timezone"):
        raise ValueError(f"Source configuration is missing field: timezone")
    if not source_configuration["timezone"] in pytz.all_timezones:
        raise ValueError(f"Invalid timezone: {source_configuration["timezone"]}")

    source_type = source_configuration["type"].lower()
    config = _create_source_configuration_object_for_type(
        source_type, source_configuration
    )

    logger.info(
        f"Adding new source with type={source_type} org_id={org_id} timezone={source_configuration["timezone"]} config={config}"
    )
    cursor.execute(
        """
        INSERT INTO configuration_sources (type, org_id, timezone, config)
        SELECT ?, ?, ?, PARSE_JSON(?);
    """,
        (source_type, org_id, source_configuration["timezone"], json.dumps(config)),
    )

    if sink_ids := source_configuration.get("sinks"):
        _associate_sinks_with_source(cursor, org_id, sink_ids)


def update_source_configuration(connection, source_configuration: dict):
    # Validate
    if not source_configuration.get("org_id"):
        raise ValueError(f"Source configuration is missing field: org_id")
    org_id = source_configuration["org_id"].lower()

    cursor = connection.cursor()
    existing = _get_source_by_org_id(cursor, org_id)
    if len(existing) != 1:
        raise Exception(
            f"Expected to find one source with org_id {org_id}, got {len(existing)}"
        )

    # Update with new source
    source = {
        "id": existing[0][0],
        "type": existing[0][1],
        "org_id": existing[0][2],
        "timezone": existing[0][3],
        "config": json.loads(existing[0][4]),
    }
    if source_type := source_configuration.get("type"):
        source["type"] = source_type.lower()
    if timezone := source_configuration.get("timezone"):
        source["timezone"] = timezone
    new_config = _create_source_configuration_object_for_type(
        source["type"], source_configuration, require_all_fields=False
    )
    for key, value in [x for x in new_config.items()]:
        # Remove any None's unless they were explicitly set in the source_configuration argument
        if value is None and key not in source_configuration:
            del new_config[key]
    source["config"].update(new_config)

    logger.info(f"Updating source with {org_id} with values {source}")
    cursor.execute(
        """
        UPDATE configuration_sources
        SET 
            type = ?,
            timezone = ?,
            config = PARSE_JSON(?)
        WHERE org_id = ?;
        """,
        (source["type"], source["timezone"], json.dumps(source["config"]), org_id),
    )
    if sink_ids := source_configuration.get("sinks"):
        _associate_sinks_with_source(cursor, org_id, sink_ids)


def _create_source_configuration_object_for_type(
    source_type: str, source_configuration: str, require_all_fields=True
) -> dict:
    """
    Each adapter type has special configuration that we represent as an object in the database.
    This function parses the source_configuration argument into that object.
    """
    match source_type:
        case "aclara":
            config = {}
            for field in [
                "sftp_host",
                "sftp_remote_data_directory",
                "sftp_local_download_directory",
                "sftp_local_known_hosts_file",
            ]:
                if not source_configuration.get(field) and require_all_fields:
                    raise ValueError(f"Source configuration is missing field: {field}")
                config[field] = source_configuration.get(field)
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
                if not source_configuration.get(field) and require_all_fields:
                    raise ValueError(f"Source configuration is missing field: {field}")
                config[field] = source_configuration.get(field)
        case "subeca":
            config = {}
        case _:
            config = {}
    return config


def remove_source_configuration(connection, org_id: str):
    cursor = connection.cursor()
    cursor.execute(
        """
        DELETE FROM configuration_sources
        WHERE org_id = ?
        """, (org_id,)
    )
    # TODO it did not remove source_sink rows.


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
    result = _get_sources_associated_with_sink_id(cursor, sink_id)
    if result:
        connected_sources = ", ".join(str(r[0]) for r in result)
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


def _get_source_by_org_id(cursor, org_id: str) -> List[List]:
    return cursor.execute(
        """
        SELECT s.id, s.type, s.org_id, s.timezone, s.config
        FROM configuration_sources s
        WHERE s.org_id = ?
    """,
        (org_id,),
    ).fetchall()


def _get_sink_by_id(cursor, sink_id: str) -> List[List]:
    return cursor.execute(
        """
        SELECT s.id, s.type
        FROM configuration_sinks s
        WHERE s.id = ?
    """,
        (sink_id,),
    ).fetchall()


def _get_sources_associated_with_sink_id(cursor, sink_id: str) -> List[List]:
    return cursor.execute(
        """
        SELECT s.org_id
        FROM configuration_source_sinks ss
        JOIN configuration_sources s ON ss.source_id = s.id
        WHERE ss.sink_id = ?
    """,
        (sink_id,),
    ).fetchall()


def _associate_sinks_with_source(cursor, org_id: str, sink_ids: List[str]):
    # Validate
    for sink_id in sink_ids:
        sinks_with_id = _get_sink_by_id(cursor, sink_id)
        if len(sinks_with_id) != 1:
            raise Exception(
                f"Expected one sink with id {sink_id}, got {len(sinks_with_id)}"
            )

    source = _get_source_by_org_id(cursor, org_id)[0]
    source_id = source[0]

    # Associate sink with source
    for sink_id in sink_ids:
        cursor.execute(
            """
            MERGE INTO configuration_source_sinks AS target
            USING (
                SELECT ? AS source_id, ? AS sink_id
            ) AS source
            ON target.source_id = source.source_id AND target.sink_id = source.sink_id
            WHEN NOT MATCHED THEN
                INSERT (source_id, sink_id)
                VALUES (source.source_id, source.sink_id);

        """,
            (source_id, sink_id),
        )
