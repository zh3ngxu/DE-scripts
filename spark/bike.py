# Import SparkSession
from pyspark.sql import SparkSession
import sys

date_str = sys.argv[1]

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[*]") \
      .appName("demo") \
      .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:1.1.0,"
                "org.apache.hadoop:hadoop-aws:3.2.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.180") \
      .getOrCreate() 

bike_df = (
    spark.read
        .option("header","true")
        .option("delimiter",",")
        .csv("s3://databricks-demo-data-bucket/day.csv")
)

bike_df.createOrReplaceTempView("bike")

df_registered_cnt_by_yrmonth = spark.sql('select substr(dteday,1,7) as yr_month, sum(registered) as registered_cnt from bike group by yr_month order by yr_month')

(
    df_registered_cnt_by_yrmonth
        .repartition(1)
        .write
        .mode("overwrite")
        .option("compression","gzip")
        .parquet(f"s3://wcddeb4-demo/result/date={date_str}")
)

