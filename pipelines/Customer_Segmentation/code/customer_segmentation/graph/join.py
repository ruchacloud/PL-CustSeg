from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def join(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    out0 = in3\
               .join(in2, "ProductID", "inner")\
               .join(in0, "Category", "inner")\
               .join(in1, "CustomerID", "inner")\
               .withColumn("Age", col("Age").cast("int"))\
               .withColumn("OrderID", col("OrderID").cast("int"))\
               .withColumn("CustomerID", col("CustomerID").cast("int"))\
               .withColumn("Quantity", col("Quantity").cast("int"))\
               .withColumn("Price", col("Price").cast("int"))

    return out0
