import logging
import requests
import json

logging.basicConfig(filename="G:/AdventsProduct/V1.1.0/AFS/ETL/logs/etl.log", format="%(asctime)s %(message)s", datefmt="%d-%m-%Y %I:%M:%S %p", level=logging.DEBUG)

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
                logging.error("Error in POST the Response to AS end Request Class!!!")
                return {"Status" : "Error"}
        except Exception as e:
            logging.error("Error in POST AS Response!!!", exc_info=True)
            logging.error(str(e))
            return {"Status": "Error"}