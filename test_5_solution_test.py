import unittest
from pyspark.sql import Row
import local_data_handler
from test_5_solution import generate_salesowners_per_company
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class TestGenerateSalesOwnersPerCompany(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_input(self):
        # Test case with valid company and salesowners data
        data = [
            Row(company_id="C1", company_name="Healthy Snacks Co", salesowners="Alice Chan, Bob Borgov"),
            Row(company_id="C2", company_name="Healthy Snacks Inc.", salesowners="Alice Chan, Charlie Aim"),
            Row(company_id="C3", company_name="Healthy Snacking Co", salesowners="David Casas"),
            Row(company_id="C4", company_name="Tropical Veg Inc", salesowners="Norberta Aisling, Eufrasia Torres"),
            Row(company_id="C5", company_name="Seafood Supplier GmbH", salesowners="Fuencisla de la Torre, Norberta Aisling"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Healthy Snacks Co", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
            Row(company_id="C2", company_name="Healthy Snacks Inc.", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
            Row(company_id="C3", company_name="Healthy Snacking Co", list_salesowners="David Casas"),
            Row(company_id="C4", company_name="Tropical Veg Inc", list_salesowners="Eufrasia Torres, Norberta Aisling"),
            Row(company_id="C5", company_name="Seafood Supplier GmbH", list_salesowners="Fuencisla de la Torre, Norberta Aisling"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_multiple_ids_same_name(self):
        # Test case with multiple company IDs but the same normalized name
        data = [
            Row(company_id="C1", company_name="Farm Fresh Co", salesowners="Eve Aslan"),
            Row(company_id="C2", company_name="Farm Fresh Ltd", salesowners="Frank Zappa"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Farm Fresh Co", salesowners="Eve Aslan, Frank Zappa"),
            Row(company_id="C2", company_name="Farm Fresh Ltd", salesowners="Eve Aslan, Frank Zappa"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_basic_uniqueness(self):
        # Test that duplicate sales owners are removed
        data = [
            Row(company_id="C1", company_name="Company A", salesowners="Alice Chan, Bob Borgov, Alice Chan"),
            Row(company_id="C1", company_name="Company A", salesowners="Bob Borgov, Charlie Aim"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Company A", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
    
    def test_sorting(self):
        # Test that sales owners are sorted alphabetically
        data = [
            Row(company_id="C1", company_name="Company A", salesowners="Charlie Aim, Alice Chan, Bob Borgov"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Company A", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_no_salesowners(self):
        # Test case where salesowners are missing
        data = [
            Row(company_id="C1", company_name="Veggie Partners Ltd", salesowners=None),
        ]
        orders_schema = StructType(
                [
                    StructField("company_id", StringType(), True),
                    StructField("company_name", StringType(), True),                    
                    StructField("salesowners", StringType(), True),
                ]
            )

        orders_df = self.spark.createDataFrame(data, schema=orders_schema)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Veggie Partners Ltd", list_salesowners=None),
        ]
        expected_df = self.spark.createDataFrame(expected_data, schema=orders_schema)

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
            Row(company_id="C1", company_name="Organic Foods Ltd", salesowners="Alice Chan, Alice Chan, Bob Borgov"),
            Row(company_id="C2", company_name="Organic Foods", salesowners="Bob Borgov, Charlie Aim"),
        ]
        orders_df = self.spark.createDataFrame(data)

        result_df = generate_salesowners_per_company(orders_df)

        # Expected output
        expected_data = [
            Row(company_id="C1", company_name="Organic Foods Ltd", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
            Row(company_id="C2", company_name="Organic Foods", list_salesowners="Alice Chan, Bob Borgov, Charlie Aim"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))



if __name__ == "__main__":
    unittest.main()