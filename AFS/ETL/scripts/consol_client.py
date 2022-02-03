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

def send_mail_to_client(receiver_email, mail_body, attachemts):
    try:
        user = 'poovendrapandi2403@gmail.com'
        app_password = "aivk hqut mpfq ukyk"
        to = receiver_email

        body = mail_body
        subject = "Consolidated Payment Amount" + " " + str(datetime.datetime.today())
        content = [body]

        with yagmail.SMTP(user, app_password) as yag:
            yag.send(to, subject, content, attachemts)
            # print("Email Sent Successfully")

        return True

    except Exception as e:
        print(e)
        return False

internal_records_properties = {
    "url": "http://localhost:50010/api/v1/alcs/generic/internal_records/?payment_date=2022-01-24&client_id=2F3MB",
    "header": {
        "Content-Type": "application/json"
    },
    "data": ""
}

m_client_properties = {
		"url": "http://localhost:50010/api/v1/alcs/generic/client_details/?client_id=2F3MB",
		"header": {
			"Content-Type": "application/json"
		},
		"data": ""
	}

internal_records_response = dr.GetResponse(internal_records_properties)
response_data = internal_records_response.get_response_data()

m_client_properties = dr.GetResponse(m_client_properties)
client_response_data = m_client_properties.get_response_data()

internal_data_list_json = json.dumps(response_data)
internal_spark_df = sqlContext.read.json(sc.parallelize([internal_data_list_json]))
# print(internal_spark_df.show())

internal_pandas_df = internal_spark_df.toPandas()

internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)

internal_grouped = internal_pandas_df.groupby(['int_reference_text_8', 'int_reference_text_7', 'int_reference_text_5', 'int_reference_text_14', 'processing_layer_name'])['int_amount_1'].sum().reset_index()

# print(internal_grouped)

internal_list = list()

for i in range(0, len(internal_grouped)):
    internal_list.append({
        "CLIENT ID": internal_grouped['int_reference_text_8'][i],
        "EMPLOYEE CODE": str(internal_grouped['int_reference_text_7'][i]),
        "EMPLOYEE NAME": internal_grouped['int_reference_text_5'][i],
        "PAYMENT DATE": '2022-01-24',
        "UTR NUMBER": internal_grouped['int_reference_text_14'][i],
        "BANK NAME": internal_grouped['processing_layer_name'][i].split(" ")[0],
        "PAYMENT AMOUNT": float(internal_grouped["int_amount_1"][i])
    })

send_df = pd.DataFrame(internal_list)

send_df.to_excel('G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/client_payment_details_2022_01_24.xlsx', index=False)

mail_body = """
    Dear Client, """ + '\n' + '\t' + """
        Please find the attachment contains the payment details as on 24/01/2022.
        
    *** This is a system generated mail. Please do not reply to this mail.
    
    Thanks and Regards,
    clientpayments@teamlase.com
"""

email_address = client_response_data[0]['email_address']

# print(send_df)

# print(internal_list)

# print(internal_pandas_df)

# send_mail_to_client(receiver_email = 'vikram@teamlease.com', internal_grouped = internal_grouped)
send_mail_to_client(receiver_email = email_address, mail_body = mail_body, attachemts = 'G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/client_payment_details_2022_01_24.xlsx')

