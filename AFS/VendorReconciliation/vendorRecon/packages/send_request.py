import logging
import requests
import json

logger = logging.getLogger("vendor_reconciliation")

class SendRequest:

    def __init__(self):
        pass

    def get_response(self, post_url, headers, data):
        try:
            response = requests.get(post_url, headers=headers, data=data)
            if response.content:
                content_data = json.loads(response.content)
                return content_data
            else:
                logging.error("Error in Getting Response in AS end Request Class!!!")
                return {"Status": "Error"}
        except Exception as e:
            logging.error("Error in Get AS Response!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}

    def post_response(self, post_url, headers, data):
        try:
            response = requests.post(post_url, headers=headers, data=data)
            if response.content:
                content_data = json.loads(response.content)
                return content_data
            else:
                logging.error("Error in POST Response to AS end Request Class!!!")
                return {"Status" : "Error"}
        except Exception as e:
            logging.error("Error in POST AS Response!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}

    def patch_response(self, post_url, headers, data):
        try:
            response = requests.patch(post_url, headers=headers, data=data)
            if response.content:
                content_data = json.loads(response.content)
                return content_data
            else:
                logging.error("Error in PATCH Response to AS end Request Class!!!")
                return {"Status": "Error"}
        except Exception as e:
            logging.error("Error in PATCH AS Response!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}

    def put_response(self, post_url, headers, data):
        try:
            response = requests.put(post_url, headers=headers, data=data)
            if response.content:
                content_data = json.loads(response.content)
                return content_data
            else:
                logging.error("Error in PUT Response to AS end Request Class!!!")
                return {"Status": "Error"}
        except Exception as e:
            logging.error("Error in PUT AS Response!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}
