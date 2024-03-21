from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def Product_Order_Data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("OrderID", StringType(), True), StructField("CustomerID", StringType(), True), StructField("ProductID", StringType(), True), StructField("Quantity", StringType(), True), StructField("Price", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/POC_Rucha/Product_Order_Data.txt")
