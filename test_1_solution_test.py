import unittest
import local_data_handler
from test_1_solution import find_crate_type_distribution
from pyspark.sql import Row

class TestCrateTypeDistribution(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_input(self):
        # Test with typical input
        data = [
            Row(company_id="C1", crate_type="Plastic"),
            Row(company_id="C1", crate_type="Wooden"),
            Row(company_id="C1", crate_type="Plastic"),
            Row(company_id="C2", crate_type="Plastic"),
            Row(company_id="C2", crate_type="Wooden"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = find_crate_type_distribution(orders_df)

        # Expected result
        expected_data = [
            Row(company_id="C1", crate_type="Plastic", count=2),
            Row(company_id="C1", crate_type="Wooden", count=1),
            Row(company_id="C2", crate_type="Plastic", count=1),
            Row(company_id="C2", crate_type="Wooden", count=1),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Compare results
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_empty_input(self):
        # Test with an empty DataFrame
        orders_df = self.spark.createDataFrame([], schema="company_id STRING, crate_type STRING")

        result_df = find_crate_type_distribution(orders_df)

        # Expected result: Empty DataFrame
        self.assertEqual(result_df.count(), 0)

    def test_single_company_single_crate_type(self):
        # Test with a single company and single crate type
        data = [Row(company_id="C1", crate_type="Plastic")] * 5
        orders_df = self.spark.createDataFrame(data)

        result_df = find_crate_type_distribution(orders_df)

        # Expected result
        expected_data = [Row(company_id="C1", crate_type="Plastic", count=5)]
        expected_df = self.spark.createDataFrame(expected_data)

        # Compare results
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_multiple_companies_overlapping_crate_types(self):
        # Test with multiple companies and overlapping crate types
        data = [
            Row(company_id="C1", crate_type="Plastic"),
            Row(company_id="C1", crate_type="Wooden"),
            Row(company_id="C2", crate_type="Plastic"),
            Row(company_id="C2", crate_type="Wooden"),
            Row(company_id="C2", crate_type="Plastic"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = find_crate_type_distribution(orders_df)

        # Expected result
        expected_data = [
            Row(company_id="C1", crate_type="Plastic", count=1),
            Row(company_id="C1", crate_type="Wooden", count=1),
            Row(company_id="C2", crate_type="Plastic", count=2),
            Row(company_id="C2", crate_type="Wooden", count=1),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Compare results
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

if __name__ == "__main__":
    unittest.main()
