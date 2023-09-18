
# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# %%
JOB_NAME = "Complex file to delimeted files transformer"
spark = SparkSession.builder.appName(JOB_NAME)\
    .config("spark.scheduler.mode", "FAIR")\
    .config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.12.0')\
    .getOrCreate()

# %%
@pandas_udf(StringType())
def column_type(s: pd.Series) -> pd.Series:
    return pd.Series([str(s.dtype)] * len(s))

spark.udf.register("column_type", column_type)

# %%
sql_script = """
select
    create_date,
    column_type(items)
    -- item['_id'], 
    -- item['_VALUE'] 
from my_data 
-- lateral view explode(items.item) t as item
"""

# %%
# works fine
read_options = {"rowTag": "my_data"}
df = spark.read\
    .format("xml")\
    .options(**read_options)\
    .load("./pyspark/xml/xml")
df.createOrReplaceTempView("my_data")
spark.sql(sql_script).show()

# %%
# Error
df2 = spark.read\
    .format("xml")\
    .options(**read_options)\
    .load("./pyspark/xml/xml/test2.xml")
df2.createOrReplaceTempView("my_data")
spark.sql(sql_script).show()

