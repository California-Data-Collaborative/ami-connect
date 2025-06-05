"""
Manually run integration test for snowflake queries.

Assumes config.yaml and secrets.yaml are set up with an adapter that uses a Snowflake sink.

Usage:
    python test/integration/snowflake_integration_test.py

"""

import datetime

import pytz

from amiadapters.config import AMIAdapterConfiguration
from amiadapters.models import GeneralMeter
from amiadapters.storage.snowflake import SnowflakeStorageSink


def run(config: AMIAdapterConfiguration):
    test_funcs = [
        test_does_not_insert_or_update_on_duplicate,
        test_inserts_when_non_matched_row_introduced,
        test_updates_when_matched_row_has_new_value,
    ]
    for fn in test_funcs:
        fn(config)
        print(f"✅ {fn.__name__} passed.")
    print(f"✅ {len(test_funcs)} tests passed.")


def setup(config):
    # Hack!
    adapter = config.adapters()[0]
    snowflake_sink = adapter.storage_sinks[0]
    assert isinstance(snowflake_sink, SnowflakeStorageSink)
    return snowflake_sink


def test_does_not_insert_or_update_on_duplicate(config: AMIAdapterConfiguration):
    snowflake_sink = setup(config)
    conn = snowflake_sink.sink_config.connection()
    cs = conn.cursor()
    test_table_name = "meters_int_test"
    row_active_from = datetime.datetime.now(tz=pytz.UTC)
    try:
        # Mimic the main table
        cs.execute(f"CREATE OR REPLACE TEMPORARY TABLE {test_table_name} LIKE meters;")

        # Verify no rows
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 0, f"Expected 0 rows, got {result}"

        # Add one meter
        meter = GeneralMeter(
            org_id="org1",
            device_id="device1",
            account_id="303022",
            location_id="303022",
            meter_id="1470158170",
            endpoint_id="130615549",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        snowflake_sink._upsert_meters(
            [meter], conn, row_active_from=row_active_from, table_name=test_table_name
        )

        # Verify added
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 1, f"Expected 1 row, got {result}"

        # Add the row again, expecting that the upsert doesn't change anything
        snowflake_sink._upsert_meters([meter], conn, table_name=test_table_name)

        # Verify a new row was not created
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 1, f"Expected 1 row, got {result}"
        # Verify row_active_from amd row_active_until values
        cs.execute(f"SELECT * FROM {test_table_name}")
        result = cs.fetchone()
        assert (
            result[-2] == row_active_from
        ), f"Unexpected row_active_from, expected {row_active_from} but got {result[-2]}"
        assert result[-1] is None, f"Unexpected row_active_until, got {result[-1]}"
    finally:
        conn.close()


def test_inserts_when_non_matched_row_introduced(config: AMIAdapterConfiguration):
    snowflake_sink = setup(config)
    conn = snowflake_sink.sink_config.connection()
    cs = conn.cursor()
    test_table_name = "meters_int_test"
    row_active_from = datetime.datetime.now(tz=pytz.UTC)
    try:
        # Mimic the main table
        cs.execute(f"CREATE OR REPLACE TEMPORARY TABLE {test_table_name} LIKE meters;")

        # Verify no rows
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 0, f"Expected 0 rows, got {result}"

        # Add one meter
        meter = GeneralMeter(
            org_id="org1",
            device_id="device1",
            account_id="303022",
            location_id="303022",
            meter_id="1470158170",
            endpoint_id="130615549",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        snowflake_sink._upsert_meters(
            [meter], conn, row_active_from=row_active_from, table_name=test_table_name
        )

        # Verify added
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 1, f"Expected 1 row, got {result}"

        # Add new meters, expecting that the upsert creates a new row
        new_device_same_org = GeneralMeter(
            org_id="org1",
            # New device_id
            device_id="device2",
            account_id="303022",
            location_id="303022",
            meter_id="2",
            endpoint_id="2",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        new_org_same_device_id = GeneralMeter(
            # New org
            org_id="org2",
            # Same device_id
            device_id="device1",
            account_id="303022",
            location_id="303022",
            meter_id="2",
            endpoint_id="2",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        snowflake_sink._upsert_meters(
            [new_device_same_org, new_org_same_device_id],
            conn,
            row_active_from=row_active_from,
            table_name=test_table_name,
        )

        # Verify new rows were created
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 3, f"Expected 3 rows, got {result}"
    finally:
        conn.close()


def test_updates_when_matched_row_has_new_value(config: AMIAdapterConfiguration):
    snowflake_sink = setup(config)
    conn = snowflake_sink.sink_config.connection()
    cs = conn.cursor()
    test_table_name = "meters_int_test"
    row_active_from = datetime.datetime.now(tz=pytz.UTC)
    try:
        # Mimic the main table
        cs.execute(f"CREATE OR REPLACE TEMPORARY TABLE {test_table_name} LIKE meters;")

        # Verify no rows
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 0, f"Expected 0 rows, got {result}"

        # Add one meter
        meter = GeneralMeter(
            org_id="org1",
            device_id="device1",
            account_id="303022",
            location_id="303022",
            meter_id="1470158170",
            endpoint_id="130615549",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=1,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        snowflake_sink._upsert_meters(
            [meter], conn, row_active_from=row_active_from, table_name=test_table_name
        )

        # Verify added
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 1, f"Expected 1 row, got {result}"

        # Update the endpoint_id, expecting that the upsert creates a new row with the new endpoint_id and sets row_active_from and row_active_until
        same_meter_new_endpoint_id = GeneralMeter(
            org_id="org1",
            device_id="device1",
            account_id="303022",
            location_id="303022",
            meter_id="2",
            endpoint_id="9090909",
            meter_install_date=datetime.datetime(
                2016, 1, 1, 23, 59, tzinfo=pytz.timezone("Europe/Rome")
            ),
            meter_size="0.625",
            meter_manufacturer="BADGER",
            multiplier=None,
            location_address="5391 E. MYSTREET",
            location_city="Apple",
            location_state="CA",
            location_zip="93727",
        )
        snowflake_sink._upsert_meters(
            [same_meter_new_endpoint_id],
            conn,
            row_active_from=row_active_from,
            table_name=test_table_name,
        )

        # Verify new row was created
        cs.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        result = cs.fetchone()[0]
        assert result == 2, f"Expected 2 rows, got {result}"

        # Verify only one row with null row_active_until
        cs.execute(
            f"SELECT COUNT(*) FROM {test_table_name} WHERE row_active_until IS NULL"
        )
        result = cs.fetchone()[0]
        assert result == 1, f"Expected 1 row, got {result}"
    finally:
        conn.close()


if __name__ == "__main__":
    config = AMIAdapterConfiguration.from_yaml("config.yaml", "secrets.yaml")
    run(config)
