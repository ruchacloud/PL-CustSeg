from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def unique_customers(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col
    out0 = in0.select(col("CustomerID"), col("Age"), col("Gender"), col("Location")).distinct()

    return out0
