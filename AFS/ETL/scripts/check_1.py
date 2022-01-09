# from pyspark import SparkConf,SparkContext
# from pyspark.sql import SQLContext, SparkSession
import pandas as pd
import re

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
def field_extraction(text):
    try:
        return text.split("/")[2]
    except Exception:
        pass

def date_extraction(text):
    try:
        text_split_colon = str(text).split(":")
        text_split_proper = text_split_colon[0] + ":" + text_split_colon[1]

        return text_split_proper
    except Exception as e:
        print(e)
        pass

def get_extract_alcs_remarks(text):
    try:
        if re.search(r'opp', str(text).lower()):
            return 'OPP'
        elif re.search(r'reim', str(text).lower()):
            return 'REIMB'
        elif re.search(r'sal', str(text).lower()):
            return 'SALARY'
    except Exception as e:
        print(e)
        return  ''

# print(df)
# check = "reference"
# check1 = 1
# df["new"] = df["Transaction Particulars"].apply(field_extraction, check=check, check1=check1)
#
# print(df["new"])

alcs_df = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/ALCS_AXIS.xlsx")
bank_df = pd.read_excel("H:/Clients/TeamLease/ALCS Letters/AXIS_18294_BANK.xlsx", skiprows=6)
bank_df["utr"] = bank_df["Transaction Particulars"].apply(field_extraction)
alcs_df["payment_date_proper"] = alcs_df["PM_Payment_Date"].apply(date_extraction)
alcs_df["payment_type"] = alcs_df["Remarks"].apply(get_extract_alcs_remarks)

print(alcs_df)


alcs_df[['Issued Amt']] = alcs_df[['Issued Amt']].apply(pd.to_numeric)
math_transformed_df_alcs = alcs_df.groupby(["payment_date_proper", "payment_type"])['Issued Amt'].sum().reset_index()

print(math_transformed_df_alcs)
print(type(math_transformed_df_alcs))

# math_transformed_df_alcs = math_transformed_series_alcs.to_frame()
# math_transformed_df_alcs['payment_date_proper'] = math_transformed_df_alcs.index

# print(math_transformed_df_alcs)
# print(type(math_transformed_df_alcs))

# Creating index for Df
# index_list = []
# for i in range(0, len(math_transformed_df_alcs)):
#     index_list.append(i)
#
# math_transformed_df_alcs.index = index_list

# print(math_transformed_df_alcs)

payment_date_aggregated_list = list()

for i in range(0, len(math_transformed_df_alcs)):
    payment_date_aggregated_list.append({
        "payment_date" : str(math_transformed_df_alcs["PM_Payment_Date"][i]),
        "issued_amount" : float(math_transformed_df_alcs["Issued Amt"][i]),
        "utr_number" : ""
    })

# print(payment_date_aggregated_list)
# print(bank_df)

# for payment_date_agg in payment_date_aggregated_list:
#     for i in range(0, len(bank_df)):
#         if ( payment_date_agg["issued_amount"] == float(bank_df["Amount (Rs.)"][i]) ) and bank_df["CR/DR"][i].lower() == "dr":
#             payment_date_agg["utr_number"] = bank_df["utr"][i]


# print(payment_date_aggregated_list)

# for payment_date_agg in payment_date_aggregated_list:
#     alcs_df.loc[alcs_df['payment_date_proper'] == payment_date_agg["payment_date"], ['UTR Number']] = payment_date_agg["utr_number"]

# print(alcs_df['UTR Number'])
# alcs_df.to_excel("H:/Clients/TeamLease/ALCS Letters/alcs_axis_output.xlsx")

# print(math_transformed_df_alcs)
# print(type(math_transformed_df_alcs))
# print(math_transformed_df_alcs[0])
# print(math_transformed_df_alcs.index(0))