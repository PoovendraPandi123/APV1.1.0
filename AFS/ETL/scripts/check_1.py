# from pyspark import SparkConf,SparkContext
# from pyspark.sql import SQLContext, SparkSession
import pandas as pd

# sc = SparkContext(master="local", appName="ETL")
# sqlContext = SQLContext(sc)
# spark = SparkSession.getActiveSession()

# df = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/AXIS_18294_BANK.xlsx", skiprows=6)
#
# # spark_df = spark.createDataFrame(df.astype(str))
# # print(spark_df.show())
#
# # spark_df.createOrReplaceTempView("date_transformed_bank_spark_df")
#
# # sql_check = sqlContext.sql("""SELECT substring_index('IFT/3053/CT0000089913/3/TEAMSAL', "'/'", 4);""")
# # sql_check = sqlContext.sql("""SELECT *, substring_index(substring_index(`Transaction Particulars`, "'/'", 3), '/', -1) from date_transformed_bank_spark_df;""")
# # print(sql_check.show())
# # for i in sql_check.take(10):
# #     print(i[0])
#
# def field_extraction(text, **kwargs):
#     try:
#         for k,v in kwargs.items():
#             if k == "check":
#                 check = v
#             if k == "check1":
#                 check1 = v
#         print(check)
#         print(check1)
#         return text.split("/")[2]
#     except Exception:
#         pass
#
# print(df)
# check = "reference"
# check1 = 1
# df["new"] = df["Transaction Particulars"].apply(field_extraction, check=check, check1=check1)
#
# print(df["new"])

alcs_df = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/ALCS_AXIS.xlsx")
alcs_df[['Issued Amt']] = alcs_df[['Issued Amt']].apply(pd.to_numeric)
math_transformed_series_alcs = alcs_df.groupby(["PM_Payment_Date"])['Issued Amt'].sum()
print(math_transformed_series_alcs)
math_transformed_df_alcs = math_transformed_series_alcs.to_frame()
math_transformed_df_alcs['PM_Payment_Date'] = math_transformed_df_alcs.index
print(math_transformed_df_alcs)
print(type(math_transformed_df_alcs))

# print(math_transformed_df_alcs)
# print(type(math_transformed_df_alcs))
# print(math_transformed_df_alcs[0])
# print(math_transformed_df_alcs.index(0))