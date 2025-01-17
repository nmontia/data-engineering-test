import local_data_handler
from pyspark.sql.functions import col, countDistinct
import warnings


local_data = local_data_handler.LocalDataHandler()
orders_df = local_data.orders_df
invocing_data_df = local_data.invocing_data_df


def assert_with_warning(condition, message):
    if not condition:
        warnings.warn(message, stacklevel=2)


class Test1_DataValidation_Checks:

    def __init__(self):
        self.orders_df = orders_df

    def order_df_company_name_company_is_one_to_one(self):
        id_to_name_group_count = (
            orders_df.groupBy("company_id")
            .agg(countDistinct("company_name").alias("distinct_company_name_count"))
            .filter(col("distinct_company_name_count") > 1)
        )

        name_to_id_group_count = (
            orders_df.groupBy("company_name")
            .agg(countDistinct("company_id").alias("distinct_company_id_count"))
            .filter(col("distinct_company_id_count") > 1)
        )

        assert_with_warning(
            name_to_id_group_count.count() == 0,
            "There is more than one company id per company name",
        )
        assert_with_warning(
            id_to_name_group_count.count() == 0,
            "There is more than one company name per company id",
        )

    def check_for_null_values(self):
        rows_with_null_data = self.orders_df.filter(col("company_id").isNull() | col("company_name").isNull() | col("crate_type").isNull())
        assert_with_warning(
            rows_with_null_data.count() == 0,
            "There are rows where company_id, company_name or crate_type are null.",
        )

    def run_data_validation_checks(self):
        self.order_df_company_name_company_is_one_to_one()
        self.check_for_null_values()


class Test2_DataValidation_Checks:

    def __init__(self):
        self.orders_df = orders_df

    def check_order_id_uniqueness(self):
        duplicate_order_count = (
            self.orders_df.groupBy("order_id").count().filter(col("count") > 1).count()
        )
        assert_with_warning(
            duplicate_order_count == 0,
            f"{duplicate_order_count} duplicate order_id(s) found",
        )

    def check_missing_contact_data(self):
        null_count = self.orders_df.filter(col("contact_data").isNull()).count()
        assert_with_warning(null_count == 0, "Some rows have null contact_data")

    def run_data_validation_checks(self):
        self.check_order_id_uniqueness()
        self.check_missing_contact_data()
