from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.createDataFrame ([ ("hello", 1), ("world", 3), ("hello world", 1), ("hello", 2), ("hello world", 3) ], ["id", "count"] )
df.show()

sc.stop()
