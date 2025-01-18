import local_data_handler
from pyspark.sql.functions import col, split, lit, explode, expr, when, sum as spark_sum, round, desc


def generate_sales_comissions(orders_df, invoicing_data_df):
    invoicing_data_df = invoicing_data_df.withColumn(
        "net_value",
        round(
            (
                col("gross_value").cast("float") / (1 + col("vat").cast("float") / 100.0)
            )  # Remove the vat amount
            / 100.0,
            2,
        ),  # Round to cents
    )

    orders_with_salesowners_df = orders_df.withColumn(
        "salesowners_list", split(col("salesowners"), ", ")
    )

    orders_with_commission_percentage_df = (
        orders_with_salesowners_df.withColumn(
            "salesowner", explode(col("salesowners_list"))
        )
        .withColumn("rank", expr("array_position(salesowners_list, salesowner)"))
        .withColumn(
            "commission_percentage",
            when(col("rank") == 1, lit(0.06))  # Main owner: 6%
            .when(col("rank") == 2, lit(0.025))  # Co-owner 1: 2.5%
            .when(col("rank") == 3, lit(0.0095))  # Co-owner 2: 0.95%
            .otherwise(lit(0)),  # Others: 0%
        )
    )

    orders_with_net_value_df = orders_with_commission_percentage_df.join(
        other=invoicing_data_df,
        how="inner",
        on=orders_with_commission_percentage_df.order_id == invoicing_data_df.order_id,
    )

    orders_with_comission_df = orders_with_net_value_df.withColumn(
        "commission_euros", round(col("net_value") * col("commission_percentage"), 2)
    )

    sales_comission_df = orders_with_comission_df.groupBy("salesowner").agg(spark_sum("commission_euros").alias("total_commission")).orderBy(desc('total_commission'))

    return sales_comission_df


if __name__ == "__main__":

    local_data = local_data_handler.LocalDataHandler()
    orders_df = local_data.orders_df
    invoicing_data_df = local_data.invoicing_data_df
    sales_comission_df = generate_sales_comissions(orders_df, invoicing_data_df)
    sales_comission_df.show()
