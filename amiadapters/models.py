import dataclasses
from datetime import datetime


@dataclasses.dataclass(frozen=True)
class GeneralMeterRead:
    """
    General model of a Meter Read at a point in time. Includes metadata we'd use to join it with
    other data.

    Attributes:
        org_id: Same as org_id for GeneralMeter
        device_id: Same as device_id for GeneralMeter
        account_id: Same as account_id for GeneralMeter. Represents account at time of measurement.
        location_id: Same as location_id for GeneralMeter. Represents account at time of measurement.
        flowtime: Time of measurement in UTC.
        register_value: Value of cumulative measured consumption at the flowtime.
        register_unit: Unit of register_value measurement
        interval_value: Value of measured consumption in the time interval since the last reading.
        interval_unit: Unit of interval_value measurement
    """

    org_id: str
    device_id: str
    account_id: str
    location_id: str
    flowtime: datetime
    register_value: float
    register_unit: str
    interval_value: float
    interval_unit: str


@dataclasses.dataclass(frozen=True)
class GeneralMeter:
    """
    General model of a Meter and its metadata, which includes information about the
    account and location it's currently associated with.

    Attributes:
        org_id: The organization that owns the meter and who we're fetching data on behalf of
        device_id: Uniquely identifies the meter(s) we're receiving measurements for. We pick
                   which meter identifier is the device_id for each AMI data provider.
        account_id: The billable utility account associated with this meter
        location_id: The location where the meter is installed
        meter_id: A unique ID from the AMI provider that identifies the meter, a.k.a. serial number
        endpoint_id: Radio / MTU ID, a.k.a. MTUID, ERT_ID, ENCID, ENDPOINT_SN, MIU_ID
        meter_install_date: UTC datetime when meter was installed. If data source doesn't provide the timezone,
                            we use the configured timezone for the org
        meter_size: size of the meter
        meter_manufacturer: Who made the meter?
        multiplier:
        location_address: street address of location
        location_city: city of location
        location_state: state of location
        location_zip: zip code of location
    """

    org_id: str
    device_id: str
    account_id: str
    location_id: str
    meter_id: str
    endpoint_id: str
    meter_install_date: datetime
    meter_size: str
    meter_manufacturer: str
    multiplier: float
    location_address: str
    location_city: str
    location_state: str
    location_zip: str
