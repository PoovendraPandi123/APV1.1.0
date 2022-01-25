from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import data_request as dr
import json
import datetime
import yagmail
import pandas as pd

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

def send_mail_to_client(receiver_email, internal_grouped):
    try:
        user = 'poovendrapandi2403@gmail.com'
        app_password = "aivk hqut mpfq ukyk"
        to = receiver_email

        body = internal_grouped
        subject = "Consolidated Payment Amount" + " " + str(datetime.datetime.today())
        content = [body]

        with yagmail.SMTP(user, app_password) as yag:
            yag.send(to, subject, content)
            # print("Email Sent Successfully")

        return True

    except Exception as e:
        print(e)
        return False

internal_records_properties = {
		"url": "http://localhost:50010/api/v1/alcs/generic/internal_records/?payment_date=2022-01-24",
		"header": {
			"Content-Type": "application/json"
		},
		"data": ""
}

internal_records_response = dr.GetResponse(internal_records_properties)
response_data = internal_records_response.get_response_data()

internal_data_list_json = json.dumps(response_data)
internal_spark_df = sqlContext.read.json(sc.parallelize([internal_data_list_json]))
# print(internal_spark_df.show())

internal_pandas_df = internal_spark_df.toPandas()

internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)

internal_grouped = internal_pandas_df.groupby(['int_reference_text_9', 'int_reference_text_14', 'processing_layer_name'])['int_amount_1'].sum().reset_index()

# print(internal_grouped)

internal_list = list()

for i in range(0, len(internal_grouped)):
    internal_list.append({
        "payment_date": '2022-01-24',
        "bank": internal_grouped['processing_layer_name'][i],
        "consolidated_amount": float(internal_grouped["int_amount_1"][i]),
        "utr_number": internal_grouped['int_reference_text_14'][i],
    })

# print(internal_list)

# print(internal_pandas_df)

send_mail_to_client(receiver_email = 'vikram@teamlease.com', internal_grouped = internal_grouped)
send_mail_to_client(receiver_email = 'poovendra@adventbizsolutions.com', internal_grouped = internal_grouped)

