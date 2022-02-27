import datetime
import yagmail
import pandas as pd
import logging

logger = logging.getLogger("alcs_recon")

def send_mai(receiver_email, mail_body, attachemts):
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

def send_mail_client(data_list, email_address, payment_from_date, payment_to_date, client_id):
    try:
        # print("Data List", data_list)
        internal_pandas_df = pd.DataFrame(data_list)
        # print(internal_pandas_df)

        internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)

        internal_grouped = internal_pandas_df.groupby(
            ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_5', 'int_reference_text_6',
                'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()

        # print("internal_grouped", internal_grouped)
        # print("internal_grouped columns", internal_grouped.columns)
        internal_list = list()

        for i in range(0, len(internal_grouped)):
            internal_list.append({
                "BANK NAME": internal_grouped["int_reference_text_1"][i],
                "DATE": internal_grouped["int_reference_date_time_1"][i],
                "ISSUED AMOUNT": float(internal_grouped["int_amount_1"][i]),
                "NAME": internal_grouped["int_reference_text_5"][i],
                "COMPANY": internal_grouped["int_reference_text_6"][i],
                "EMPLOYEE CODE": str(internal_grouped["int_reference_text_7"][i]),
                "CLIENT ID": internal_grouped["int_reference_text_8"][i],
                "REMARKS": internal_grouped["int_reference_text_9"][i],
                "IFSC CODES": internal_grouped["int_reference_text_11"][i],
                "UTR NUMBER": internal_grouped["int_reference_text_14"][i],
                "DEBIT DATE": internal_grouped["int_reference_date_time_2"][i]
            })

        send_df = pd.DataFrame(internal_list)
        # print("send_df", send_df)

        file_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/" + client_id + "_payment_details_from_" + payment_from_date.replace("-", "_") + "_to_" + payment_to_date.replace("-", "_") + ".xlsx"

        send_df.to_excel(file_path, index=False)
        #
        # mail_body = """
        #     Dear Client, """ + """\n""" + """\t""" + """
        #         Please find the attachment contains the payment details as on 24/01/2022.
        #
        #     *** This is a system generated mail. Please do not reply to this mail.
        #
        #     Thanks and Regards,
        #     clientpayments@teamlase.com
        # """

        mail_body = """<pre>Dear Client,""" + """\n\t""" + """The attached file contains the payment details from """ + payment_from_date + """ to """ + payment_to_date + """.\n\n""" \
                    + """Please Find Attached""" + """\n\n""" + """***This is a system generated mail. Please do not reply to this mail.""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + """clientpayments@teamlease.com"""

        if send_mai(receiver_email=email_address, mail_body=mail_body,
                            attachemts=file_path):

            return True
        else:
            return False
    except Exception:
        logger.error("Error in Send Mail Client!!!", exc_info=True)
        return False