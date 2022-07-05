import send_request
import logging
import json

def get_execute_etl():
    try:

        execute_batch_files_url = "http://localhost:50012/api/v1/consol_files/source/"

        headers = {
            "Content-Type": "application/json"
        }

        etl = send_request.SendRequest()

        payload = json.dumps({
            "tenants_id": 1,
            "groups_id": 1,
            "entities_id": 1,
            "m_processing_layer_id": 2,
            "m_processing_sub_layer_id": 11
        })

        execute_batch_files_response = etl.get_response(post_url=execute_batch_files_url, headers=headers, data=payload)

        print("execute_batch_files_response")
        print(execute_batch_files_response)

        return {"Status": "Success", "data": execute_batch_files_response}

    except Exception:
        logging.error("Error in Get Execute ETL!!!", exc_info=True)
        return {"Status": "Error"}



def post_execute_file_upload(data, colm_name):
    try:



        return {"Status": "Success", "data": execute_batch_files_response}

    except Exception:
        logging.error("Error in Get Execute ETL!!!", exc_info=True)
        return {"Status": "Error"}




def post_execute_storage_upload(data):
    try:

        execute_batch_files_url = "http://localhost:50012/api/v1/consol_files/fileupload/"

        headers = {
            "Content-Type": "application/json"
        }

        etl = send_request.SendRequest()

        payload = json.dumps({
            "tenants_id": 1,
            "groups_id": 1,
            "entities_id": 1,
            "module_id": 1

        })

        execute_batch_files_response = etl.post_response(post_url=execute_batch_files_url, headers=headers, data=data)

        print("execute_batch_files_response")
        print(execute_batch_files_response)


        return {"Status": "Success", "data": execute_batch_files_response}

    except Exception:
        logging.error("Error in Get Execute ETL!!!", exc_info=True)
        return {"Status": "Error"}






#if __name__ == "__main__":
 #   get_execute_etl()
