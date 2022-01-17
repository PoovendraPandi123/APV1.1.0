from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import pandas as pd
import numpy as np

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

columns = ['Transaction ID', 'Value Date', 'Txn Posted Date', 'ChequeNo.', 'Description', 'Cr/Dr', 'Transaction Amount(INR)', 'Available Balance(INR)']

data = pd.read_excel("G:/AdventsProduct/V1.1.0/AFS/Sources/Data/BANK_ICICI240_ALCS\input\ICICI 240_17012022.xlsx", usecols=columns, skiprows=6)[columns]

data_proper = data.replace(np.nan, '')
spark_df = spark.createDataFrame(data_proper.astype(str))
print(spark_df.show())

validate_df_rdd = spark_df.rdd

print(validate_df_rdd.count())

for i in validate_df_rdd.take(10):
    print(i[0])