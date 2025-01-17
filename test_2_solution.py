import local_data_handler
import data_validation_checks
from pyspark.sql.functions import col, concat_ws, lit, explode, coalesce


def associate_order_with_contact_full_name(orders_df):
    exploded_df = orders_df.withColumn(
        "exploded_contact_data",
        explode(col("contact_data")),
    )

    order_contact_name_df = exploded_df.withColumn(
        "contact_full_name",
        coalesce(  # Handle missing or null values
            concat_ws(
                " ",
                col("exploded_contact_data.contact_name"),  # Explode contact_name
                col("exploded_contact_data.contact_surname"),  # Explode contact_surname
            ),
            lit("John Doe"),  # Use "John Doe" as a placeholder
        ),
    ).select("order_id", "contact_full_name")

    return order_contact_name_df


if __name__ == "__main__":
    # Outputs warning that company id and company name do not match 1 to 1 (casing and spelling issues)
    data_validation_checks.Test2_DataValidation_Checks().run_data_validation_checks()

    local_data = local_data_handler.LocalDataHandler()
    orders_df = local_data.orders_df
    df_1 = associate_order_with_contact_full_name(orders_df)
    df_1.show()
