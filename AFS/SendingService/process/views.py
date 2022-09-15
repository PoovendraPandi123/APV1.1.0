from django.shortcuts import render
import logging
import json
import re
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import requests
from .packages import outlook_properties as outlook_prop

# Create your views here.

logger = logging.getLogger("sending_service")


def get_read_mail_from_outlook(request, *args, **kwargs):
    try:
        if request.method == "GET":

            # body = request.body.decode('utf-8')
            # data = json.loads(body)

            read_mail_address = "vendorreco@thermaxglobal.com"
            parent_path = "D:/AdventsProduct/V1.1.0/AFS/SendingService/static"
            outlook_properties = outlook_prop.ReadOutlookMail(mail_id=read_mail_address, parent_path=parent_path)
            outlook_read_response = outlook_properties.get_outlook_read_response()

            if outlook_read_response:
                files_list = outlook_properties.get_files_list()
                for file in files_list:
                    file_path = file["file_name"].replace("\\\\", "/")
                    file_name = open(file_path, "rb")

                    download_data_url = "http://10.100.2.181:50013/api/v1/vendor_recon/get_download_data_from_outlook/"

                    payload_file = {
                        "externalFileName": file_name
                    }

                    payload = {
                        "mail_data": file["subject"]
                    }

                    requests.post(download_data_url, files=payload_file, data=payload, verify=False)
                    file_name.close()

                return JsonResponse({"Status": "Success"})
            else:
                return JsonResponse({"Status": "Error"})
        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Read Mail From Outlook Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_send_mail_to_vendor_through_outlook(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "send_mail_vendor_list":
                    send_mail_vendor_list = v
                if k == "from_date":
                    from_date = v
                if k == "to_date":
                    to_date = v

            send_mail_count = 0

            for vendor in send_mail_vendor_list:

                mail_subject = 'Vendor Reconciliation of Thermax - ' + vendor["division"] + ' - Division - Balance for period from ' + from_date + " to " + to_date + " - Vendor Code - " + vendor["vendor_code"]
                mail_body = """Hi """ + vendor["vendor_name"] + """,""" + """\n""" + """\t\t""" + """Closing Balance for the period is """ + vendor["closing_balance"] + """\n\n""" + """Kindly confirm with 'YES' if Matches else attach the statement for the same period and reply back.""" + """\n\n\n\n\n"""
                cc_email_address = ''

                outlook_properties = outlook_prop.SendOutlookMail(
                    receiver_email_address = vendor["contact_email"],
                    mail_subject = mail_subject,
                    html_body = '',
                    mail_body = mail_body,
                    cc_email_address = cc_email_address
                )
                outlook_send_response = outlook_properties.get_outlook_send_response()
                if outlook_send_response:
                    send_mail_count += 1
                    continue
                    #return JsonResponse({"Status": "Success"})
                else:
                    logger.info("Error in Sending Email!!!")
                    logger.info(vendor['contact_email'])

            if send_mail_count > 0:
                return JsonResponse({"Status": "Success"})
            else:
                return JsonResponse({"Status": "Error"})

        return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Send Mail to Vendor Through Outlook Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})