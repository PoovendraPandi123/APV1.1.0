from .process.packages import send_request as sr
import logging
import json

def get_read_mail():
    try:
        read_mail_url = "http://localhost:50014/api/v1/sending_service/get_read_mail_from_outlook/"

        headers = {
            "Content-Type": "application/json"
        }

        payload = {}

        read_mail = sr.SendRequest()

        read_mail_response = read_mail.get_response(post_url=read_mail_url, headers=headers, data=payload)

        print("read_mail_response")
        print(read_mail_response)

        return "Success"

    except Exception:
        logging.error("Error in Get Read Mail Function!!!", exc_info=True)
        return "Error"

if __name__ == "__main__":
    get_read_mail()