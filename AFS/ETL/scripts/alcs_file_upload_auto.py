import data_request as dr
import os
import api_properties as api
import alcs_file_functions as af
import json
import logging

if __name__ == "__main__":
    config_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
    # API Calls
    api_properties_file = os.path.join(config_folder, "api_calls.json")
    api_properties = api.APIProperties(property_folder=config_folder, property_file=api_properties_file)
    api_properties_data = api_properties.get_api_properties()

    batch_file_properties = api_properties_data.get("batch_file_properties", "")
    if batch_file_properties:
        batch_file_properties["url"] = batch_file_properties["url"].replace("batch", "batch_all")
        batch_all_files = dr.GetResponse(batch_file_properties)
        batch_all_files_list = batch_all_files.get_response_data()
        if len(batch_all_files_list) > 0:
            for batch_files in batch_all_files_list:
                if batch_files["file_path"].split("/")[-3] == 'ALCS':
                    alcs_file_path = batch_files["file_path"]
                    alcs_file_id = batch_files["id"]
                    alcs_file_validation = af.ALCSConsolidatedFileValidation(file_path = alcs_file_path)
                    alcs_validate_output = alcs_file_validation.get_validate_output()
                    if alcs_validate_output:
                        print("File is Wright!!!")
                        alcs_consolidated_data = alcs_file_validation.get_consolidated_alcs_data()

                        af.ALCSConsolidatedFileSplit.store_alcs_file(
                            data_frame = '',
                            bank_column = '',
                            alcs_bank_name = '',
                            alcs_file_path = '',
                            alcs_file_name = ''
                        )
                    else:
                        print("File is Wrong!!!")
                        file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                        # print(file_uploads_unique_record_properties)
                        if file_uploads_unique_record_properties:
                            patch_payload = json.dumps({
                                "status": "VALIDATION ERROR",
                                "comments": "Error in PM Payment Date!!!",
                                "is_processed": 1
                            })
                            file_uploads_unique_record_properties["url"] = file_uploads_unique_record_properties["url"].replace("{file_id}", str(alcs_file_id))
                            file_uploads_unique_record_properties["data"] = patch_payload
                            file_uploads_unique_record = dr.PatchResponse(file_uploads_unique_record_properties)
                            file_uploads_unique_record_response = file_uploads_unique_record.get_patch_response()
                            if file_uploads_unique_record_response["id"] == alcs_file_id:
                                logging.info("Error File Status Updated!!!")
                                logging.info(file_uploads_unique_record_response["id"])

