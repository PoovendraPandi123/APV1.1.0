import datetime
import yagmail
import pandas as pd
import logging

logger = logging.getLogger("alcs_recon")

def send_mail(receiver_email, mail_body, attachments):
    try:
        # print("receiver_email", receiver_email)
        user = 'poovendrapandi2403@gmail.com'
        app_password = "aivk hqut mpfq ukyk"
        to = receiver_email
        #cc = "poovendra@adventbizsolutions.com"

        body = mail_body
        subject = "Consolidated Payment Release Details" + " " + str(datetime.datetime.today())
        content = [body]

        with yagmail.SMTP(user, app_password) as yag:
            yag.send(to=to, subject=subject, contents=content, attachments=attachments)
            # yag.send(to, cc, subject, content)
            # print("Email Sent Successfully")

        return True

    except Exception as e:
        print(e)
        return False

# def send_mail_client(data_list, email_address, payment_from_date, payment_to_date, client_id):
#     try:
#         # print("Data List", data_list)
#         internal_pandas_df = pd.DataFrame(data_list)
#         # print(internal_pandas_df)
#
#         internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)
#
#         internal_grouped = internal_pandas_df.groupby(
#             ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6',
#                 'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()
#
#         # print("internal_grouped", internal_grouped)
#         # print("internal_grouped columns", internal_grouped.columns)
#         internal_list = list()
#
#         for i in range(0, len(internal_grouped)):
#             internal_list.append({
#                 "BANK NAME": internal_grouped["int_reference_text_1"][i],
#                 "DATE": internal_grouped["int_reference_date_time_1"][i],
#                 "ISSUED AMOUNT": float(internal_grouped["int_amount_1"][i]),
#                 "EMP ACCOUNT NUMBER": internal_grouped["int_reference_text_4"][i],
#                 "NAME": internal_grouped["int_reference_text_5"][i],
#                 "COMPANY": internal_grouped["int_reference_text_6"][i],
#                 "EMPLOYEE CODE": str(internal_grouped["int_reference_text_7"][i]),
#                 "CLIENT ID": internal_grouped["int_reference_text_8"][i],
#                 "REMARKS": internal_grouped["int_reference_text_9"][i],
#                 "INVOICE NUMBER": internal_grouped["int_reference_text_10"][i],
#                 "IFSC CODES": internal_grouped["int_reference_text_11"][i],
#                 "UTR NUMBER": internal_grouped["int_reference_text_14"][i],
#                 "DEBIT DATE": internal_grouped["int_reference_date_time_2"][i]
#             })
#
#         send_df = pd.DataFrame(internal_list)
#         # print("send_df", send_df)
#
#         file_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/" + client_id + "_payment_details_from_" + payment_from_date.replace("-", "_") + "_to_" + payment_to_date.replace("-", "_") + ".xlsx"
#
#         writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
#         send_df.to_excel(writer, sheet_name="Payments")
#
#         writer.save()
#
#         # send_df.to_excel(file_path, index=False)
#         #
#         # mail_body = """
#         #     Dear Client, """ + """\n""" + """\t""" + """
#         #         Please find the attachment contains the payment details as on 24/01/2022.
#         #
#         #     *** This is a system generated mail. Please do not reply to this mail.
#         #
#         #     Thanks and Regards,
#         #     clientpayments@teamlase.com
#         # """
#
#         mail_body = """<pre>Dear Client,""" + """\n\t""" + """The attached file contains the payment details from """ + payment_from_date + """ to """ + payment_to_date + """.\n\n""" \
#                     + """Please Find Attached""" + """\n\n""" + """***This is a system generated mail. Please do not reply to this mail.""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + """clientpayments@teamlease.com"""
#
#         if send_mai(receiver_email=email_address, mail_body=mail_body,
#                             attachemts=file_path):
#
#             return True
#         else:
#             return False
#     except Exception:
#         logger.error("Error in Send Mail Client!!!", exc_info=True)
#         return False

def send_mail_client(data_list, email_address, payment_from_date, payment_to_date, client_id, rejections_data_list):
    try:
        # print("Data List", data_list)

        if len(rejections_data_list) > 0 and len(data_list) > 0:

            internal_pandas_df = pd.DataFrame(data_list)
            internal_rejections_pandas_df = pd.DataFrame(rejections_data_list)
            # print(internal_pandas_df)

            internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)
            internal_rejections_pandas_df['int_amount_1'] = internal_rejections_pandas_df['int_amount_1'].apply(pd.to_numeric)

            internal_grouped = internal_pandas_df.groupby(
                ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6',
                    'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()

            internal_rejected_grouped = internal_rejections_pandas_df.groupby(
                ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6',
                    'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()
            # print("internal_grouped", internal_grouped)
            # print("internal_grouped columns", internal_grouped.columns)
            internal_list = list()
            internal_rejections_list = list()

            for i in range(0, len(internal_grouped)):
                internal_list.append({
                    "TL BANK NAME": internal_grouped["int_reference_text_1"][i],
                    "DATE": internal_grouped["int_reference_date_time_1"][i],
                    "ISSUED AMOUNT": float(internal_grouped["int_amount_1"][i]),
                    "EMP ACCOUNT NUMBER": internal_grouped["int_reference_text_4"][i],
                    "NAME": internal_grouped["int_reference_text_5"][i],
                    "COMPANY": internal_grouped["int_reference_text_6"][i],
                    "EMPLOYEE CODE": str(internal_grouped["int_reference_text_7"][i]),
                    "CLIENT ID": internal_grouped["int_reference_text_8"][i],
                    "REMARKS": internal_grouped["int_reference_text_9"][i],
                    "INVOICE NUMBER": internal_grouped["int_reference_text_10"][i],
                    "IFSC CODES": internal_grouped["int_reference_text_11"][i],
                    "UTR NUMBER": internal_grouped["int_reference_text_14"][i],
                    "DEBIT DATE": internal_grouped["int_reference_date_time_2"][i]
                })

            for j in range(0, len(internal_rejected_grouped)):
                internal_rejections_list.append({
                    "TL BANK NAME": internal_rejected_grouped["int_reference_text_1"][j],
                    "DATE": internal_rejected_grouped["int_reference_date_time_1"][j],
                    "ISSUED AMOUNT": float(internal_rejected_grouped["int_amount_1"][j]),
                    "EMP ACCOUNT NUMBER": internal_rejected_grouped["int_reference_text_4"][j],
                    "NAME": internal_rejected_grouped["int_reference_text_5"][j],
                    "COMPANY": internal_rejected_grouped["int_reference_text_6"][j],
                    "EMPLOYEE CODE": str(internal_rejected_grouped["int_reference_text_7"][j]),
                    "CLIENT ID": internal_rejected_grouped["int_reference_text_8"][j],
                    "REMARKS": internal_rejected_grouped["int_reference_text_9"][j],
                    "INVOICE NUMBER": internal_rejected_grouped["int_reference_text_10"][j],
                    "IFSC CODES": internal_rejected_grouped["int_reference_text_11"][j],
                    "UTR NUMBER": internal_rejected_grouped["int_reference_text_14"][j],
                    "DEBIT DATE": internal_rejected_grouped["int_reference_date_time_2"][j]
                })

            send_df = pd.DataFrame(internal_list)
            send_rejected_df = pd.DataFrame(internal_rejections_list)
            # print("send_df", send_df)

            file_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/" + client_id + "_payment_details_from_" + payment_from_date.replace("-", "_") + "_to_" + payment_to_date.replace("-", "_") + ".xlsx"

            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            send_df.to_excel(writer, sheet_name="Payments", index=False)
            send_rejected_df.to_excel(writer, sheet_name="Rejections", index=False)
            writer.save()

            # send_df.to_excel(file_path, index=False)
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
                        + """Please Find Attached""" + """\n\n""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + """client_communication@teamlease.com""" + """\n\n\n\n\n\n\n""" + """***This is a system generated mail. Please do not reply to this mail."""

            if send_mail(receiver_email=email_address, mail_body=mail_body,
                                attachments=file_path):

                return True
            else:
                return False

        elif len(data_list) > 0 and len(rejections_data_list) == 0:
            internal_pandas_df = pd.DataFrame(data_list)
            internal_pandas_df['int_amount_1'] = internal_pandas_df['int_amount_1'].apply(pd.to_numeric)

            internal_grouped = internal_pandas_df.groupby(
                ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6',
                    'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()

            internal_list = list()

            for i in range(0, len(internal_grouped)):
                internal_list.append({
                    "TL BANK NAME": internal_grouped["int_reference_text_1"][i],
                    "DATE": internal_grouped["int_reference_date_time_1"][i],
                    "ISSUED AMOUNT": float(internal_grouped["int_amount_1"][i]),
                    "EMP ACCOUNT NUMBER": internal_grouped["int_reference_text_4"][i],
                    "NAME": internal_grouped["int_reference_text_5"][i],
                    "COMPANY": internal_grouped["int_reference_text_6"][i],
                    "EMPLOYEE CODE": str(internal_grouped["int_reference_text_7"][i]),
                    "CLIENT ID": internal_grouped["int_reference_text_8"][i],
                    "REMARKS": internal_grouped["int_reference_text_9"][i],
                    "INVOICE NUMBER": internal_grouped["int_reference_text_10"][i],
                    "IFSC CODES": internal_grouped["int_reference_text_11"][i],
                    "UTR NUMBER": internal_grouped["int_reference_text_14"][i],
                    "DEBIT DATE": internal_grouped["int_reference_date_time_2"][i]
                })

            send_df = pd.DataFrame(internal_list)

            file_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/" + client_id + "_payment_details_from_" + payment_from_date.replace("-", "_") + "_to_" + payment_to_date.replace("-", "_") + ".xlsx"

            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            send_df.to_excel(writer, sheet_name="Payments", index=False)
            writer.save()

            mail_body = """<pre>Dear Client,""" + """\n\t""" + """The attached file contains the payment details from """ + payment_from_date + """ to """ + payment_to_date + """.\n\n""" \
                        + """Please Find Attached""" + """\n\n""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + """client_communication@teamlease.com""" + """\n\n\n\n\n\n\n""" + """***This is a system generated mail. Please do not reply to this mail."""

            if send_mail(receiver_email=email_address, mail_body=mail_body,
                         attachments=file_path):

                return True
            else:
                return False

        elif len(rejections_data_list) > 0 and len(data_list) == 0:
            internal_rejections_pandas_df = pd.DataFrame(rejections_data_list)

            internal_rejections_pandas_df['int_amount_1'] = internal_rejections_pandas_df['int_amount_1'].apply(pd.to_numeric)

            internal_rejected_grouped = internal_rejections_pandas_df.groupby(
                ['int_reference_text_1', 'int_reference_date_time_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6',
                    'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'])['int_amount_1'].sum().reset_index()


            internal_rejections_list = list()

            for j in range(0, len(internal_rejected_grouped)):
                internal_rejections_list.append({
                    "TL BANK NAME": internal_rejected_grouped["int_reference_text_1"][j],
                    "DATE": internal_rejected_grouped["int_reference_date_time_1"][j],
                    "ISSUED AMOUNT": float(internal_rejected_grouped["int_amount_1"][j]),
                    "EMP ACCOUNT NUMBER": internal_rejected_grouped["int_reference_text_4"][j],
                    "NAME": internal_rejected_grouped["int_reference_text_5"][j],
                    "COMPANY": internal_rejected_grouped["int_reference_text_6"][j],
                    "EMPLOYEE CODE": str(internal_rejected_grouped["int_reference_text_7"][j]),
                    "CLIENT ID": internal_rejected_grouped["int_reference_text_8"][j],
                    "REMARKS": internal_rejected_grouped["int_reference_text_9"][j],
                    "INVOICE NUMBER": internal_rejected_grouped["int_reference_text_10"][j],
                    "IFSC CODES": internal_rejected_grouped["int_reference_text_11"][j],
                    "UTR NUMBER": internal_rejected_grouped["int_reference_text_14"][j],
                    "DEBIT DATE": internal_rejected_grouped["int_reference_date_time_2"][j]
                })

            send_rejected_df = pd.DataFrame(internal_rejections_list)

            file_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/mail/" + client_id + "_payment_details_from_" + payment_from_date.replace("-", "_") + "_to_" + payment_to_date.replace("-", "_") + ".xlsx"

            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')

            send_rejected_df.to_excel(writer, sheet_name="Rejections", index=False)
            writer.save()

            mail_body = """<pre>Dear Client,""" + """\n\t""" + """The attached file contains the payment details from """ + payment_from_date + """ to """ + payment_to_date + """.\n\n""" \
                        + """Please Find Attached""" + """\n\n""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + """client_communication@teamlease.com""" + """\n\n\n\n\n\n\n""" + """***This is a system generated mail. Please do not reply to this mail."""

            if send_mail(receiver_email=email_address, mail_body=mail_body,
                         attachments=file_path):

                return True
            else:
                return False

    except Exception:
        logger.error("Error in Send Mail Client!!!", exc_info=True)
        return False