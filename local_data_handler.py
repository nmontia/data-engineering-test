from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, regexp_replace, from_json, explode


class LocalDataHandler:
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Ensure only one instance of the class is created
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, sparkSession=None):
        if not self._initialized:
            print("Initializing the local data handler.")
            self._sparkSession = (
                sparkSession
                or SparkSession.builder.appName("AuctionDataAnalysis").getOrCreate()
            )
            self._orders_df = None
            self._invoicing_data_df = None
            self._initialized = True

    def parse_contact_data(self, df, contact_data_schema):
        return df.withColumn(
            "contact_data",
            from_json(
                regexp_replace(
                    regexp_replace(col("contact_data"), '""', '"'), '^"|"$', ""
                ),  # Replace doubled quotes with single quotes
                contact_data_schema,
            ),
        )

    @property
    def orders_df(self):
        # Only load orders_df if not already loaded
        if self._orders_df is None:
            print("Loading orders csv file into dataframe.")
            orders_path = "resources/orders.csv"
            orders_schema = StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("date", StringType(), True),
                    StructField("company_id", StringType(), True),
                    StructField("company_name", StringType(), True),
                    StructField("crate_type", StringType(), True),
                    StructField("contact_data", StringType(), True),
                    StructField("salesowners", StringType(), True),
                ]
            )
            self._orders_df = self._sparkSession.read.csv(
                orders_path, header=True, schema=orders_schema, sep=";", quote='"'
            )
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
            self._orders_df = self.parse_contact_data(
                self._orders_df, contact_data_schema
            )
        return self._orders_df

    @orders_df.setter
    def orders_df(self, new_value):
        self._orders_df = new_value

    @property
    def invoicing_data_df(self):
        # Only load invoicing_data_df if not already loaded
        if self._invoicing_data_df is None:
            print("Loading invoicing data json file into dataframe.")
            invoicing_data_path = "resources/invoicing_data.json"
            invoice_schema = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("orderId", StringType(), True),
                    StructField("companyId", StringType(), True),
                    StructField(
                        "grossValue", StringType(), True
                    ),
                    StructField(
                        "vat", StringType(), True
                    )
                ]
            )
            json_schema = StructType(
                [
                    StructField(
                        "data",
                        StructType(
                            [StructField("invoices", ArrayType(invoice_schema), True)]
                        ),
                    )
                ]
            )
            self._invoicing_data_df = self._sparkSession.read.json(
                invoicing_data_path, multiLine=True, schema=json_schema
            )
            self._invoicing_data_df = self._invoicing_data_df.select(
                explode(col("data.invoices")).alias("invoice")
            )
            self._invoicing_data_df = self._invoicing_data_df.select(
                col("invoice.id").alias("id"),
                col("invoice.orderId").alias("order_id"),
                col("invoice.companyId").alias("company_id"),
                col("invoice.grossValue").cast("float").alias("gross_value"),
                col("invoice.vat").cast("float").alias("vat"),
            )
            return self._invoicing_data_df

    @invoicing_data_df.setter
    def invoicing_data_df(self, new_value):
        print("Setting a new value for orders_df...")
        self.invoicing_data_df = new_value
