import local_data_handler
from pyspark.sql.functions import (
    col,
    collect_set,
    trim,
    lower,
    concat_ws,
    regexp_replace,
    array_min,
    explode,
    split,
    sort_array,
)


def generate_salesowners_per_company(orders_df):
    orders_df = orders_df.withColumn(
        "normalised_company_name",
        trim(lower(col("company_name"))),  # remove whitespaces and make names lowercase
    ).withColumn(
        "normalised_company_name",
        regexp_replace(
            col("normalised_company_name"),
            r"(?i)\s+(co|inc|ltd|gmbh|c\.o\.|c\.o)\.?$",
            "",
        ),
        # Remove all business designations from the company names and assume that if the rest of the name matches, they are the same entity
        # Some of the names are similar enough that they could be the same company (e.g if the first word is the same),
        # but that would have to be given as a business rule so the assumption is that they are not the same.
    )

    deduplicated_df = orders_df.groupBy("normalised_company_name").agg(
        collect_set("company_id").alias("unique_company_ids"),
        collect_set("company_name").alias("unique_company_names"),
    )

    mapped_df = deduplicated_df.withColumn(
        "primary_company_id",
        array_min(
            col("unique_company_ids")
        ),  # We need one arbitrary id to be primary - we pick minimum
    )

    updated_df = orders_df.join(
        mapped_df.select(
            col("primary_company_id"),
            col("normalised_company_name").alias("mapped_normalised_company_name"),
        ),
        on=[col("mapped_normalised_company_name") == col("normalised_company_name")],
        how="inner",
    ).drop("mapped_normalised_company_name")

    exploded_df = updated_df.withColumn(
        "sales_owner_individual",
        explode(split(col("salesowners"), r",\s*")),  # Split on commas and trim spaces
    )

    salesowners_per_unique_company_df = exploded_df.groupBy(
        "primary_company_id", "normalised_company_name"
    ).agg(
        concat_ws(", ", sort_array(collect_set(col("sales_owner_individual")))).alias(
            "list_salesowners"
        )
    )

    salesowners_per_company_df= updated_df.join(salesowners_per_unique_company_df, how="left", 
                   on=salesowners_per_unique_company_df.normalised_company_name == updated_df.normalised_company_name
                    ).select("company_id", "company_name", "list_salesowners").distinct()

    return salesowners_per_company_df


if __name__ == "__main__":

    local_data = local_data_handler.LocalDataHandler()
    orders_df = local_data.orders_df
    salesowners_per_company_df = generate_salesowners_per_company(orders_df)
    salesowners_per_company_df.show()
