import unittest
from pyspark.sql import Row
import local_data_handler
from test_5_solution import generate_salesowners_per_company
from pyspark.sql.functions import explode, col, coalesce, lit, concat_ws


class TestGenerateSalesOwnersPerCompany(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_input(self):
        # Test case with valid company and salesowners data
        data = [
            Row(company_id="C1", company_name="Healthy Snacks Co", salesowners="Alice, Bob"),
            Row(company_id="C2", company_name="Healthy Snacks c.o.", salesowners="Alice, Charlie"),
            Row(company_id="C3", company_name="Healthy Snacks Ltd", salesowners="David"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(primary_company_id="C1", list_salesowners="Alice, Bob, Charlie, David"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_multiple_ids_same_name(self):
        # Test case with multiple company IDs but the same normalized name
        data = [
            Row(company_id="C1", company_name="Farm Fresh Co", salesowners="Eve"),
            Row(company_id="C2", company_name="Farm Fresh Ltd", salesowners="Frank"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(primary_company_id="C1", list_salesowners="Eve, Frank"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_no_salesowners(self):
        # Test case where salesowners are missing
        data = [
            Row(company_id="C1", company_name="Veggie Partners Ltd", salesowners=None),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(primary_company_id="C1", list_salesowners=""),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_empty_input(self):
        # Test case with no input data
        orders_df = self.spark.createDataFrame([], schema="company_id STRING, company_name STRING, salesowners STRING")

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output: Empty DataFrame
        self.assertEqual(result_df.count(), 0)

    def test_duplicate_salesowners(self):
        # Test case with duplicate salesowners
        data = [
            Row(company_id="C1", company_name="Organic Foods Ltd", salesowners="Alice, Alice, Bob"),
            Row(company_id="C2", company_name="Organic Foods", salesowners="Bob, Charlie"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(primary_company_id="C1", list_salesowners="Alice, Bob, Charlie"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))



if __name__ == "__main__":
    unittest.main()