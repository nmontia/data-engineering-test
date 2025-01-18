import local_data_handler
from pyspark.sql.functions import col, concat_ws, lit, explode, coalesce

def associate_order_with_contact_address(orders_df):
    exploded_cleaned_df = (
        orders_df.withColumn(
            "exploded_contact_data",
            explode(col("contact_data")),
        )
        .withColumn("city", coalesce(col("exploded_contact_data.city"), lit("Unknown")))
        .withColumn(
            "postal_code", coalesce(col("exploded_contact_data.cp"), lit("UNK00"))
        )
    )
    order_contact_address_df = exploded_cleaned_df.withColumn(
        "contact_address", concat_ws(", ", col("city"), col("postal_code"))
    ).select("order_id", "contact_address")

    return order_contact_address_df


if __name__ == "__main__":

    local_data = local_data_handler.LocalDataHandler()
    orders_df = local_data.orders_df
    df_2 = associate_order_with_contact_address(orders_df)
    df_2.show()
