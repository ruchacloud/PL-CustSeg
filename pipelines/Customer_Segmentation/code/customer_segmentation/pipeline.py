from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customer_segmentation.config.ConfigStore import *
from customer_segmentation.udfs.UDFs import *
from prophecy.utils import *
from customer_segmentation.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Product_Order_Data = Product_Order_Data(spark)
    df_Product_Detail = Product_Detail(spark)
    df_Customer_Data = Customer_Data(spark)
    df_Category_Description = Category_Description(spark)
    df_parse_csv_to_category_description = parse_csv_to_category_description(spark, df_Category_Description)
    df_join = join(
        spark, 
        df_parse_csv_to_category_description, 
        df_Customer_Data, 
        df_Product_Order_Data, 
        df_Product_Detail
    )
    df_frequency_by_customer_id = frequency_by_customer_id(spark, df_join)
    df_total_purchase_amount_by_customer = total_purchase_amount_by_customer(spark, df_join)
    df_unique_customers = unique_customers(spark, df_join)
    unique_customers_1(spark, df_unique_customers)
    df_avg_basket_size_by_order = avg_basket_size_by_order(spark, df_join)
    average_basket_size(spark, df_avg_basket_size_by_order)
    frequency(spark, df_frequency_by_customer_id)
    total_purchased(spark, df_total_purchase_amount_by_customer)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Customer_Segmentation")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Customer_Segmentation", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Customer_Segmentation")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
