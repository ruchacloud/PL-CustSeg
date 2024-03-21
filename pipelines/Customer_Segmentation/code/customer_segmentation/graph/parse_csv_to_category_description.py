from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *

def parse_csv_to_category_description(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]).toDF(["value"])
    out0 = out0\
               .withColumn("Category", expr("split(value, ',', 2)[0]"))\
               .withColumn("Description", expr("substring(value, length(split(value, ',', 2)[0])+2)"))\
               .drop("value")

    return out0
