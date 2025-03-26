from abc import ABC, abstractmethod
import dataclasses
from datetime import datetime
import json


class BaseAMIAdapter(ABC):
    """
    Abstraction of an AMI data source. If you're adding a new type of data source,
    you'll inherit from this class and implement its abstract methods. That should
    set you up to include it in our data pipeline.
    """

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    def datetime_from_iso_str(self, datetime_str: str, org_timezone: str) -> datetime:
        # TODO timezones
        if datetime_str:
            result = datetime.fromisoformat(datetime_str)
        else:
            result = None
        return result

    def map_meter_size(self, size: str) -> str:
        """
        Map an AMI data provider meter's size to one
        of our generalized values. Return None if it can't be mapped.
        """
        mapping = {
            '3/8"': "0.375",
        }
        return mapping.get(size)

    def get_unit_of_measure(self, unit_of_measure: str) -> str:
        """
        Map an AMI data provider meter read's unit of measure to one
        of our generalized values. Return None if it can't be mapped.
        """
        mapping = {
            "CCF": GeneralMeterUnitOfMeasure.CCF,
            "Gallon": GeneralMeterUnitOfMeasure.GAL,
        }
        return mapping.get(unit_of_measure)


class GeneralMeterUnitOfMeasure:
    """
    Normalized values for a meter's unit of measure. All AMI-provided
    values should be mapped to one of these.
    """

    CCF = "CCF"
    GAL = "Gallon"


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
    location_state: str
    location_zip: str


@dataclasses.dataclass(frozen=True)
class GeneralMeterRead:
    """
    General model of a Meter Read at a point in time. Includes metadata we'd use to join it with
    other data.
    """

    meter_id: str
    account_id: str
    location_id: str
    flowtime: datetime
    raw_value: float
    raw_unit: str


class DataclassJSONEncoder(json.JSONEncoder):
    """
    Helps write data classes into JSON.
    """

    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class GeneralModelJSONEncoder(DataclassJSONEncoder):
    """
    Standardizes how we serialize our general models into JSON.
    """

    def default(self, o):
        # Datetimes use isoformat
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)
