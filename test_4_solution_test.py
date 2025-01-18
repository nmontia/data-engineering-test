import unittest
from pyspark.sql import Row
import local_data_handler
from test_4_solution import generate_sales_comissions
from pyspark.sql.functions import explode, col, coalesce, lit, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class TestGenerateSalesComissions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_input(self):
        # Test case with valid salesowners and invoicing data
        orders_data = [
            Row(order_id="O1", salesowners="Alice, Bob"),
            Row(order_id="O2", salesowners="Alice, Charlie, Bob"),
        ]
        invoicing_data = [
            Row(order_id="O1", gross_value=12225, vat=20),
            Row(order_id="O2", gross_value=8662, vat=16),
        ]

        orders_df = self.spark.createDataFrame(orders_data)
        invoicing_df = self.spark.createDataFrame(invoicing_data)

        result_df = generate_sales_comissions(orders_df, invoicing_df)

        # Expected output
        expected_data = [
            Row(salesowner="Alice", total_commission=10.59),  # 6% of (122.25/1.2) + 6% of (86.62/1.16) -rounds down
            Row(salesowner="Bob", total_commission=3.26),     # 2.5% of (122.25/1.2) + 0.95% of (86.62/1.16) - rounds up
            Row(salesowner="Charlie", total_commission=1.87),  # 2.5% of (86.62/1.16) - rounds up
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_no_salesowners(self):
        # Test case with no salesowners
        orders_data = [Row(order_id="O1", salesowners=None)]
        invoicing_data = [Row(order_id="O1", gross_value=12000, vat=20)]
        orders_schema = StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("salesowners", StringType(), True),
                ]
            )

        orders_df = self.spark.createDataFrame(orders_data, schema=orders_schema)
        invoicing_df = self.spark.createDataFrame(invoicing_data)

        result_df = generate_sales_comissions(orders_df, invoicing_df)
        # Expected output
        expected_data = []
        expected_df = self.spark.createDataFrame(expected_data, schema="salesowner STRING, total_commission DOUBLE")
        self.assertEqual(result_df.count(), expected_df.count())

    def test_only_main_owner(self):
        # Test case with only one owner
        orders_data = [Row(order_id="O1", salesowners="Alice")]
        invoicing_data = [Row(order_id="O1", gross_value=10000, vat=20)]

        orders_df = self.spark.createDataFrame(orders_data)
        invoicing_df = self.spark.createDataFrame(invoicing_data)

        result_df = generate_sales_comissions(orders_df, invoicing_df)

        # Expected output
        expected_data = [Row(salesowner="Alice", total_commission=5.0)]  # 6% of (100/1.2)
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_zero_gross_value(self):
        # Test case with zero gross value
        orders_data = [Row(order_id="O1", salesowners="Alice, Bob")]
        invoicing_data = [Row(order_id="O1", gross_value=0, vat=20)]

        orders_df = self.spark.createDataFrame(orders_data)
        invoicing_df = self.spark.createDataFrame(invoicing_data)

        result_df = generate_sales_comissions(orders_df, invoicing_df)

        # Expected output
        expected_df = self.spark.createDataFrame([Row(salesowner="Alice", total_commission=0), Row(salesowner="Bob", total_commission=0)])

        self.assertEqual(result_df.count(), expected_df.count())



if __name__ == "__main__":
    unittest.main()