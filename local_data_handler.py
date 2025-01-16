from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


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

    @property
    def orders_df(self):
        # Only load orders_df if not already loaded
        if self._orders_df is None:
            print("Loading orders csv file into dataframe.")
            orders_path = "resources/orders.csv"
            orders_schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("date", StringType(), True),
                StructField("company_id", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("crate_type", StringType(), True),
                StructField("contact_data", StringType(), True),
                StructField("salesowners", StringType(), True),
            ])
            self._orders_df = self._sparkSession.read.csv(
                orders_path, header=True, schema=orders_schema, sep=";", quote='"'
            )
        return self._orders_df

    @orders_df.setter
    def orders_df(self, new_value):
        self._orders_df = new_value

    @property
    def invocing_data_df(self):
        # Only load invoicing_data_df if not already loaded
        if self._invoicing_data_df is None:
            print("Loading invoicing data json file into dataframe.")
            invoicing_data_path = "resources/invoicing_data.json"
            self._invoicing_data_df = self._sparkSession.read.json(invoicing_data_path)
        return self._invoicing_data_df

    @invocing_data_df.setter
    def invocing_data_df(self, new_value):
        print("Setting a new value for orders_df...")
        self.invocing_data_df = new_value
