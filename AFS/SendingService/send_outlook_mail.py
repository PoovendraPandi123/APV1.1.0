from .process.packages import send_request as sr
import logging
import json

def get_send_mail():
    try:
        send_mail_url = "http://localhost:50014/api/v1/sending_service/get_send_mail_to_vendor_through_outlook/"

        headers = {
            "Content-Type": "application/json"
        }

        payload = json.dumps({
            "receiver_email_address": "poovendra@adventbizsolutions.com",
            "mail_subject": "Check Send Mail",
            "mail_body": "Testing!!!",
            "cc_email_address": "vinaya@adventbizsolutions.com"
        })

        send_mail = sr.SendRequest()

        send_mail_response = send_mail.post_response(post_url=send_mail_url, headers=headers, data=payload)

        print("send_mail_response")
        print(send_mail_response)

        return "Success"

    except Exception:
        logging.error("Error in Get Send Mail Function!!!", exc_info=True)
        return "Error"

if __name__ == "__main__":
    get_send_mail()
