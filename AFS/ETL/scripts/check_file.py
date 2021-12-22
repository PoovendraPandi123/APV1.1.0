from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)

df = sc.parallelize([(1,2), (3,0)]).toDF(["col1", "col2"])
print(type(df))