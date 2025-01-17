import unittest
from pyspark.sql import Row
import local_data_handler
from test_2_solution import associate_order_with_contact_full_name
from pyspark.sql.functions import col, regexp_replace, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


class TestOrdersWithFullName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_case(self):
        # Typical case: name and surname present
        data = [
            Row(
                order_id="order_1",
                contact_data='[{ "contact_name":"Curtis", "contact_surname":"Jackson" }]',
            ),
        ]

        contact_data_schema = ArrayType(
            StructType(
                [
                    StructField("contact_name", StringType(), True),
                    StructField("contact_surname", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("cp", StringType(), True),
                ]
            )
        )
        df = self.data_handler.parse_contact_data(
            self.spark.createDataFrame(data), contact_data_schema=contact_data_schema
        )

        # Expected output
        expected_data = [
            Row(order_id="order_1", contact_full_name="Curtis Jackson"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Run function and compare results
        result_df = associate_order_with_contact_full_name(df)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_missing_name_or_surname(self):
        # Missing contact_name or contact_surname
        data = [
            Row(order_id="order_2", contact_data=[{"contact_name": "Curtis"}]),
            Row(order_id="order_3", contact_data=[{"contact_surname": "Jackson"}]),
        ]
        df = self.spark.createDataFrame(data)

        # Expected output
        expected_data = [
            Row(order_id="order_2", contact_full_name="John Doe"),
            Row(order_id="order_3", contact_full_name="John Doe"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Run function and compare results
        result_df = associate_order_with_contact_full_name(df)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_missing_contact_data(self):
        # Missing contact_data
        data = [
            Row(order_id="order_4", contact_data=[{"city": "Gotham"}]),
        ]
        df = self.spark.createDataFrame(data)

        # Expected output
        expected_data = [
            Row(order_id="order_4", contact_full_name="John Doe"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        result_df = associate_order_with_contact_full_name(df)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_multiple_contacts(self):
        # Multiple contacts in the JSON string
        data = [
            Row(
                order_id="order_5",
                contact_data=[
                    {"contact_name": "Bruce", "contact_surname": "Wayne"},
                    {"contact_name": "Clark", "contact_surname": "Kent"},
                ],
            ),
        ]
        df = self.spark.createDataFrame(data)

        expected_data = [
            Row(order_id="order_5", contact_full_name="Bruce Wayne"),
            Row(order_id="order_5", contact_full_name="Clark Kent"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        result_df = associate_order_with_contact_full_name(df)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_empty_dataframe(self):
        # Empty input DataFrame
        df = self.spark.createDataFrame(
            [],
            schema=StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("contact_data", ArrayType(
                        StructType(
                            [
                                StructField("contact_name", StringType(), True),
                                StructField("contact_surname", StringType(), True),
                                StructField("city", StringType(), True),
                                StructField("cp", StringType(), True),
                            ]
                        )
                    ), True),
                ]
            ),
        )

        # Expected output: Empty DataFrame
        result_df = associate_order_with_contact_full_name(df)

        self.assertEqual(result_df.count(), 0)

if __name__ == "__main__":
    unittest.main()
