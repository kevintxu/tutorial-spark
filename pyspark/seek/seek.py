# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

# %%
class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("ElectricChargePointsETLJob")
                                          .getOrCreate())

    def extract(self):
        ops = {"header": True}
        df = self.spark_session.read.format("csv").options(**ops).load(self.input_path)
        df = df.withColumn("PluginDuration", col("PluginDuration").cast(FloatType()))
        return df

    def transform(self, df):
        df.createOrReplaceTempView("electric_chargepoints")
        sql_text = """select 
                CPID as chargepoint_id,
                round(max(PluginDuration), 2) as max_duration,
                round(avg(PluginDuration), 2) as avg_duration
            from electric_chargepoints
            group by CPID
        """
        return self.spark_session.sql(sql_text)

    def load(self, df):
        df.write.format("parquet").save(self.output_path)

    def run(self):
        self.load(self.transform(self.extract()))

# %%
