from datetime import datetime
import logging
import json
from typing import Dict, List, Tuple

from amiadapters.configuration.models import (
    ConfiguredAMISourceTypes,
    PipelineConfiguration,
    SourceConfigBase,
)

logger = logging.getLogger(__name__)


def get_configuration(snowflake_connection) -> Tuple[
    List[Dict],
    List[Dict],
    PipelineConfiguration,
    Dict,
    List[Dict],
    Dict,
]:
    """
    Given a Snowflake connection, load all raw configuration objects from the database.
    We return as dicts and lists to match the YAML config system's API.
    """
    tables = [
        "configuration_pipeline",
        "configuration_sources",
        "configuration_source_sinks",
        "configuration_sinks",
        "configuration_sink_checks",
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
        source.update(json.loads(row["config"]) if row["config"] else {})
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
        sink["checks"] = checks_by_sink_id.get(row["id"], [])
        sinks.append(sink)

    pipeline_config = _parse_pipeline_configuration(all_config)

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

    return sources, sinks, pipeline_config, notifications, backfills


def _fetch_table(cursor, table_name):
    """Fetch all rows from a configuration table and return as list of dicts."""
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [col[0].lower() for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_pipeline_configuration(all_config: dict) -> PipelineConfiguration:
    if len(all_config["configuration_pipeline"]) != 1:
        raise ValueError(
            f"Expected one row for configuration_pipeline, got {len(all_config["configuration_pipeline"])}"
        )

    row = all_config["configuration_pipeline"][0]
    return PipelineConfiguration(
        intermediate_output_type=row["intermediate_output_type"],
        intermediate_output_s3_bucket=row["intermediate_output_s3_bucket"],
        intermediate_output_dev_profile=row["intermediate_output_dev_profile"],
        intermediate_output_local_output_path=row[
            "intermediate_output_local_output_path"
        ],
        should_run_post_processor=row["run_post_processors"],
        should_publish_load_finished_events=row["publish_load_finished_events"],
        metrics_type=row["metrics_type"],
    )


def add_source_configuration(connection, source_configuration: dict):
    """
    Add a new source configuration to the database. Ensure that the source does not already exist.
    Validate using the source configuration models.
    """
    if not source_configuration.get("org_id"):
        raise ValueError(f"Source configuration is missing field: org_id")
    org_id = source_configuration["org_id"].lower()

    cursor = connection.cursor()
    existing = _get_source_by_org_id(cursor, org_id)
    if len(existing) != 0:
        raise Exception(f"Source with org_id {org_id} already exists")

    if not source_configuration.get("type"):
        raise ValueError(f"Source configuration is missing field: type")
    source_configuration["type"] = source_configuration["type"].lower()

    if not source_configuration.get("sinks"):
        source_configuration["sinks"] = []

    # Validate
    new_source = _create_and_validate_source_config_from_dict(source_configuration)
    new_source.validate()

    config = new_source.type_specific_config_dict()

    logger.info(f"Adding new source {new_source}")
    cursor.execute(
        """
        INSERT INTO configuration_sources (type, org_id, timezone, config)
        SELECT ?, ?, ?, PARSE_JSON(?);
    """,
        (
            new_source.type,
            new_source.org_id,
            new_source.timezone.zone,
            json.dumps(config),
        ),
    )

    # Add sink associations
    if sink_ids := source_configuration.get("sinks"):
        _associate_sinks_with_source(cursor, new_source.org_id, sink_ids)


def update_source_configuration(connection, new_values: dict):
    if not new_values.get("org_id"):
        raise ValueError(f"Source configuration is missing field: org_id")
    org_id = new_values["org_id"].lower()

    # Retrieve existing so that we can preserve previous values that aren't
    # being updated
    cursor = connection.cursor()
    existing = _get_source_by_org_id(cursor, org_id)
    if len(existing) != 1:
        raise Exception(
            f"Expected to find one source with org_id {org_id}, got {len(existing)}"
        )

    # Start with existing values
    updated_source_config = {
        "type": existing[0][1],
        "org_id": existing[0][2],
        "timezone": existing[0][3],
    }
    updated_source_config.update(json.loads(existing[0][4]))

    if updated_source_config["type"] != new_values.get(
        "type", updated_source_config["type"]
    ):
        raise ValueError(
            "Changing source type is not supported. Create a new source or manually update in database."
        )

    # Overwrite with any provided new values, preserving the existing values
    updated_source_config.update(new_values)

    # Validate
    new_source = _create_and_validate_source_config_from_dict(updated_source_config)
    new_config = new_source.type_specific_config_dict()

    for key, value in [x for x in new_config.items()]:
        # Remove any None values unless they were explicitly set in the source_configuration argument
        if value is None and key not in new_values:
            del new_config[key]

    logger.info(f"Updating source in database with {org_id} with values {new_source}")
    cursor.execute(
        """
        UPDATE configuration_sources
        SET 
            timezone = ?,
            config = PARSE_JSON(?)
        WHERE org_id = ?;
        """,
        (new_source.timezone.zone, json.dumps(new_config), org_id),
    )

    # Handle sink associations
    if sink_ids := new_values.get("sinks"):
        _associate_sinks_with_source(cursor, org_id, sink_ids)


def _create_and_validate_source_config_from_dict(
    source_configuration: dict,
) -> SourceConfigBase:
    """
    Validate new source configuration by creating and validating config object for this type of source
    This automagically ensures that all and only valid fields are present
    """
    cls = ConfiguredAMISourceTypes.get_config_type_for_source_type(
        source_configuration["type"]
    )
    c = source_configuration.copy()
    # The following are not configured using this function, so spoof them to satisfy the constructor
    c["secrets"] = None
    c["task_output_controller"] = None
    c["metrics"] = None
    c["sinks"] = []
    new_source = cls.from_dict(c)
    new_source.validate()
    return new_source


def remove_source_configuration(connection, org_id: str):
    org_id = org_id.lower()
    cursor = connection.cursor()
    existing = _get_source_by_org_id(cursor, org_id)
    if len(existing) != 1:
        raise Exception(
            f"Expected to find one source with org_id {org_id}, got {len(existing)}"
        )
    existing_backfills = _get_backfills_by_org_id(cursor, org_id)
    if len(existing_backfills) > 0:
        raise Exception(
            f"Cannot remove source with {len(existing_backfills)} associated backfills."
        )
    try:
        source_id = existing[0][0]
        cursor.execute("BEGIN")
        # Delete from child tables first
        cursor.execute(
            "DELETE FROM configuration_source_sinks WHERE source_id = ?",
            (source_id,),
        )
        # Delete from parent table
        cursor.execute(
            "DELETE FROM configuration_sources WHERE id = ?",
            (source_id,),
        )
        cursor.execute("COMMIT")
    except Exception as e:
        logger.error("Rolling back", e)
        cursor.execute("ROLLBACK")


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
        task_output_configuration.get("type") == "s3"
        and not task_output_configuration["dev_profile"]
    ):
        raise ValueError(
            f"Task output configuration with type s3 is missing field: dev_profile"
        )
    if (
        task_output_configuration.get("type") == "local"
        and not task_output_configuration["local_output_path"]
    ):
        raise ValueError(
            f"Task output configuration with type local is missing field: local_output_path"
        )

    cursor = connection.cursor()
    _ensure_pipeline_config_row_exists(cursor)
    cursor.execute(
        """
        UPDATE configuration_pipeline
        SET
            intermediate_output_type = ?,
            intermediate_output_s3_bucket = ?,
            intermediate_output_dev_profile = ?,
            intermediate_output_local_output_path = ?
    """,
        (
            task_output_configuration["type"],
            task_output_configuration.get("s3_bucket"),
            task_output_configuration.get("dev_profile"),
            task_output_configuration.get("local_output_path"),
        ),
    )


def update_post_processor_configuration(connection, should_run_post_processor: bool):
    # Validate
    if should_run_post_processor is None:
        raise ValueError("should_run_post_processor argument is None")
    cursor = connection.cursor()
    _ensure_pipeline_config_row_exists(cursor)
    cursor.execute(
        """
        UPDATE configuration_pipeline
        SET run_post_processors = ?
    """,
        (should_run_post_processor,),
    )


def _ensure_pipeline_config_row_exists(cursor):
    """
    Ensure there's at least one row in the pipeline config table (does nothing if there is already a row)
    """
    cursor.execute(
        """
        INSERT INTO configuration_pipeline (id, intermediate_output_type, run_post_processors)
        SELECT 1, 's3', true
        WHERE NOT EXISTS (SELECT 1 FROM configuration_pipeline)
        """
    )


def update_backfill_configuration(connection, backfill_configuration: dict):
    # Validate
    for field in [
        "org_id",
        "start_date",
        "end_date",
        "interval_days",
        "schedule",
    ]:
        if not backfill_configuration.get(field):
            raise ValueError(f"Backfill configuration is missing field: {field}")

    org_id = backfill_configuration["org_id"].lower()

    cursor = connection.cursor()
    cursor.execute(
        """
        MERGE INTO configuration_backfills AS target
        USING (
            SELECT ? AS org_id, ? AS start_date, ? AS end_date, ? AS interval_days, ? AS schedule
        ) AS source
        ON target.org_id = source.org_id AND target.start_date = source.start_date AND target.end_date = source.end_date
        WHEN MATCHED THEN
            UPDATE SET
                interval_days = source.interval_days,
                schedule = source.schedule
        WHEN NOT MATCHED THEN
            INSERT (org_id, start_date, end_date, interval_days, schedule)
            VALUES (source.org_id, source.start_date, source.end_date, source.interval_days, source.schedule)

    """,
        (
            org_id,
            backfill_configuration["start_date"],
            backfill_configuration["end_date"],
            backfill_configuration["interval_days"],
            backfill_configuration["schedule"],
        ),
    )


def remove_backfill_configuration(
    connection, org_id: str, start_date: datetime, end_date: datetime
):
    cursor = connection.cursor()
    cursor.execute(
        """
        DELETE
        FROM configuration_backfills
        WHERE org_id = ? AND start_date = ? AND end_date = ?
    """,
        (
            org_id,
            start_date,
            end_date,
        ),
    )


def update_notification_configuration(connection, notification_configuration: dict):
    # Validate
    for field in [
        "event_type",
        "sns_arn",
    ]:
        if not notification_configuration.get(field):
            raise ValueError(f"Notification configuration is missing field: {field}")

    cursor = connection.cursor()
    cursor.execute(
        """
        MERGE INTO configuration_notifications AS target
        USING (
            SELECT ? AS event_type, ? AS sns_arn
        ) AS source
        ON target.event_type = source.event_type
        WHEN MATCHED THEN
            UPDATE SET
                sns_arn = source.sns_arn
        WHEN NOT MATCHED THEN
            INSERT (event_type, sns_arn)
            VALUES (source.event_type, source.sns_arn)
    """,
        (
            notification_configuration["event_type"],
            notification_configuration["sns_arn"],
        ),
    )


def add_data_quality_check_configurations(connection, check_configuration: dict):
    # Validate
    for field in [
        "sink_id",
        "check_names",
    ]:
        if not check_configuration.get(field):
            raise ValueError(f"Check configuration is missing field: {field}")

    sink_id = check_configuration["sink_id"]
    cursor = connection.cursor()
    if not _get_sink_by_id(cursor, sink_id):
        raise ValueError(f"No sink found for id: {sink_id}")
    for check_name in check_configuration["check_names"]:
        logger.info(f"Adding check {check_name}")
        cursor.execute(
            """
            MERGE INTO configuration_sink_checks AS target
            USING (
                SELECT ? AS sink_id, ? AS check_name
            ) AS source
            ON target.sink_id = source.sink_id AND target.check_name = source.check_name
            WHEN NOT MATCHED THEN
                INSERT (sink_id, check_name)
                VALUES (source.sink_id, source.check_name)
        """,
            (
                sink_id,
                check_name,
            ),
        )


def remove_data_quality_check_configurations(connection, checks_configuration: dict):
    # Validate
    for field in [
        "sink_id",
        "check_names",
    ]:
        if not checks_configuration.get(field):
            raise ValueError(f"Check configuration is missing field: {field}")

    sink_id = checks_configuration["sink_id"]
    cursor = connection.cursor()
    if not _get_sink_by_id(cursor, sink_id):
        raise ValueError(f"No sink found for id: {sink_id}")
    for check_name in checks_configuration["check_names"]:
        logger.info(f"Removing check {check_name}")
        cursor.execute(
            """
            DELETE FROM configuration_sink_checks
            WHERE sink_id = ? AND check_name = ?
        """,
            (
                sink_id,
                check_name,
            ),
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


def _get_backfills_by_org_id(cursor, org_id: str) -> List[List]:
    return cursor.execute(
        """
        SELECT b.id, b.org_id, b.start_date, b.end_date, b.interval_days, b.schedule
        FROM configuration_backfills b
        WHERE b.org_id = ?
    """,
        (org_id,),
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
