from amiadapters.utils.conversions import map_reading
from test.base_test_case import BaseTestCase


class TestMapReading(BaseTestCase):

    def test_valid_ccf_conversion(self):
        value, unit = map_reading(12.5, "CCF")
        self.assertEqual(value, 1250)
        self.assertEqual(unit, "CF")

    def test_valid_cf_conversion(self):
        value, unit = map_reading(1200, "CF")
        self.assertEqual(value, 1200)
        self.assertEqual(unit, "CF")

    def test_valid_gal_conversion(self):
        value, unit = map_reading(2000, "Gallon")
        self.assertAlmostEqual(value, 267.36, delta=0.01)
        self.assertEqual(unit, "CF")

        value, unit = map_reading(2000, "Gallons")
        self.assertAlmostEqual(value, 267.36, delta=0.01)
        self.assertEqual(unit, "CF")

        value, unit = map_reading(2000, "GAL")
        self.assertAlmostEqual(value, 267.36, delta=0.01)
        self.assertEqual(unit, "CF")

    def test_valid_kgal_conversion(self):
        value, unit = map_reading(5, "KGAL")
        self.assertAlmostEqual(value, 668.405, delta=0.01)
        self.assertEqual(unit, "CF")

    def test_none_reading(self):
        value, unit = map_reading(None, "CCF")
        self.assertIsNone(value)
        self.assertIsNone(unit)

    def test_none_unit(self):
        value, unit = map_reading(10.0, None)
        self.assertEqual(value, 10.0)
        self.assertIsNone(unit)

    def test_none_both(self):
        value, unit = map_reading(None, None)
        self.assertIsNone(value)
        self.assertIsNone(unit)

    def test_unrecognized_unit(self):
        with self.assertRaises(ValueError, msg="Unrecognized unit of measure: Pounds"):
            map_reading(5.0, "Pounds")
