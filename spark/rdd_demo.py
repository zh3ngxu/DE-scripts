
# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[*]") \
      .appName("demo") \
      .getOrCreate() 

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)

rdd.repartition(1).saveAsTextFile("result")