from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def frequency_by_customer_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import count
    out0 = in0.groupBy("CustomerID").count().withColumnRenamed("count", "Frequency")

    return out0
