import unittest
from pyspark.sql import Row
import local_data_handler
from test_3_solution import associate_order_with_contact_address
from pyspark.sql.functions import explode, col, coalesce, lit, concat_ws


class TestAssociateOrderWithContactFullName(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up Spark session for testing
        cls.data_handler = local_data_handler.LocalDataHandler()
        cls.spark = cls.data_handler._sparkSession

    def test_typical_input(self):
        # Test case with valid city and postal code
        data = [
            Row(order_id="O1", contact_data=[{"city": "New York", "cp": "10001"}]),
            Row(order_id="O2", contact_data=[{"city": "Los Angeles", "cp": "90001"}]),
        ]
        orders_df = self.spark.createDataFrame(data)
        result_df = associate_order_with_contact_address(orders_df)

        # Expected output
        expected_data = [
            Row(order_id="O1", contact_address="New York, 10001"),
            Row(order_id="O2", contact_address="Los Angeles, 90001"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_missing_city(self):
        # Test case with missing city
        data = [
            Row(order_id="O1", contact_data=[{"city": None, "cp": "10001"}]),
            Row(order_id="O2", contact_data=[{"cp": "90001"}]),
        ]
        orders_df = self.spark.createDataFrame(data)
        result_df = associate_order_with_contact_address(orders_df)

        # Expected output
        expected_data = [
            Row(order_id="O1", contact_address="Unknown, 10001"),
            Row(order_id="O2", contact_address="Unknown, 90001"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_missing_postal_code(self):
        # Test case with missing postal code
        data = [
            Row(order_id="O1", contact_data=[{"city": "New York", "cp": None}]),
            Row(order_id="O2", contact_data=[{"city": "Los Angeles"}]),
        ]
        orders_df = self.spark.createDataFrame(data)
        result_df = associate_order_with_contact_address(orders_df)

        # Expected output
        expected_data = [
            Row(order_id="O1", contact_address="New York, UNK00"),
            Row(order_id="O2", contact_address="Los Angeles, UNK00"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_missing_city_and_postal_code(self):
        # Test case with both city and postal code missing
        data = [
            Row(order_id="O1", contact_data=[{"city": None, "cp": None}]),
            Row(order_id="O2", contact_data=[{}]),
        ]
        orders_df = self.spark.createDataFrame(data)
        result_df = associate_order_with_contact_address(orders_df)

        # Expected output
        expected_data = [
            Row(order_id="O1", contact_address="Unknown, UNK00"),
            Row(order_id="O2", contact_address="Unknown, UNK00"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_multiple_contact_data(self):
        # Test case with multiple contact_data entries
        data = [
            Row(order_id="O1", contact_data=[
                {"city": "New York", "cp": "10001"},
                {"city": "Los Angeles", "cp": "90001"}
            ]),
        ]
        orders_df = self.spark.createDataFrame(data)
        result_df = associate_order_with_contact_address(orders_df)

        # Expected output
        expected_data = [
            Row(order_id="O1", contact_address="New York, 10001"),
            Row(order_id="O1", contact_address="Los Angeles, 90001"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))


if __name__ == "__main__":
    unittest.main()