from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import pandas as pd

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

# df = sc.parallelize([(1,2), (3,0)]).toDF(["col1", "col2"])
# print(type(df))

# parDF1 = spark.read.parquet("G:/AdventsProduct/V1.1.0/AFS/ETL/data/output/out_table.parquet")
# print(parDF1.show())
df = pd.read_parquet('G:/AdventsProduct/V1.1.0/AFS/ETL/data/output/out_table.parquet', engine='pyarrow')
print(df)

spark_df = spark.createDataFrame(df)
print(spark_df.show())