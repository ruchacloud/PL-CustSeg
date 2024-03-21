from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def Customer_Data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CustomerID", StringType(), True), StructField("Age", StringType(), True), StructField("Gender", StringType(), True), StructField("Location", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/POC_Rucha/Customer_Data.txt")
