from typing import Tuple

from amiadapters.models import GeneralMeterUnitOfMeasure


def map_reading(reading: float, original_unit_of_measure: str) -> Tuple[float, str]:
    """
    All readings values should be mapped to CF. This function maps a reading from
    an arbitrary unit to CF, and returns the converted value along with the new unit of measure..
    """
    if reading is None:
        return None, None

    if original_unit_of_measure is None:
        return reading, None

    multiplier = 1
    match original_unit_of_measure.upper():
        case GeneralMeterUnitOfMeasure.CUBIC_FEET:
            multiplier = 1
        case GeneralMeterUnitOfMeasure.HUNDRED_CUBIC_FEET:
            multiplier = 100
        case GeneralMeterUnitOfMeasure.GALLON | GeneralMeterUnitOfMeasure.GAL | GeneralMeterUnitOfMeasure.GALLONS:
            multiplier = 0.133680546  # 1 / 7.48052
        case GeneralMeterUnitOfMeasure.KILO_GALLON:
            multiplier = 133.680546  # 1000 * 1 / 7.48052
        case _:
            raise ValueError(
                f"Unrecognized unit of measure: {original_unit_of_measure}"
            )

    # 8 was picked arbitrarily, a balance between our Gallon multiplier and a precision
    # that reflects increases in a fraction of a gallon
    value = round(reading * multiplier, 8)

    return value, GeneralMeterUnitOfMeasure.CUBIC_FEET
