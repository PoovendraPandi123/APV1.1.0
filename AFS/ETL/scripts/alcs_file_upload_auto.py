import re
from pathlib import Path
import data_request as dr
import os
import api_properties as api
import alcs_file_functions as af
import json
import logging
import alcs_file_properties as ap
import bank_file_functions as bf
import bank_file_properties as bp
import shutil
from datetime import datetime

def get_update_file_upload_status(file_uploads_unique_record_properties, file_id, status, comments):
    try:
        patch_payload = json.dumps({
            "status": status,
            "comments": comments,
            "is_processed": 1
        })
        file_uploads_unique_record_properties_url_split = file_uploads_unique_record_properties["url"].split("/")
        file_uploads_unique_record_properties_url_split[-2] = str(file_id)
        file_uploads_unique_record_properties_url_proper = "/".join(file_uploads_unique_record_properties_url_split)
        file_uploads_unique_record_properties["url"] = file_uploads_unique_record_properties_url_proper
        file_uploads_unique_record_properties["data"] = patch_payload
        file_uploads_unique_record = dr.PatchResponse(file_uploads_unique_record_properties)
        file_uploads_unique_record_response = file_uploads_unique_record.get_patch_response()
        if file_uploads_unique_record_response["id"] == file_id:
            logging.info("Individual File Status updated in DB!!!")
            logging.info(file_uploads_unique_record_response["id"])
            return "Success"
        else:
            return "Error"
    except Exception as e:
        print(e)
        logging.error("Error in Get Update File Upload Status Function!!!", exc_info=True)

if __name__ == "__main__":
    try:
        config_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
        # API Calls
        api_properties_file = os.path.join(config_folder, "api_calls.json")
        api_properties = api.APIProperties(property_folder=config_folder, property_file=api_properties_file)
        api_properties_data = api_properties.get_api_properties()

        batch_file_properties = api_properties_data.get("batch_file_properties", "")
        file_uploads_all_properties = api_properties_data.get("file_uploads_all_properties", "")
        if batch_file_properties:
            batch_file_properties["url"] = batch_file_properties["url"].replace("batch", "batch_all")
            batch_all_files = dr.GetResponse(batch_file_properties)
            batch_all_files_list = batch_all_files.get_response_data()
            if len(batch_all_files_list) > 0:
                for batch_files in batch_all_files_list:
                    if batch_files["file_path"].split("/")[-3] == 'ALCS':
                        alcs_file_path = batch_files["file_path"]
                        alcs_file_id = batch_files["id"]
                        alcs_user_id = batch_files["created_by"]
                        alcs_upload_time = batch_files["created_date"]
                        alcs_file_validation = af.ALCSConsolidatedFileValidation(file_path = alcs_file_path)
                        alcs_validate_output = alcs_file_validation.get_validate_output()
                        if alcs_validate_output:
                            print("File is Wright!!!")
                            alcs_consolidated_data = alcs_file_validation.get_consolidated_alcs_data()
                            alcs_property_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
                            alcs_property_file = os.path.join(alcs_property_folder, "alcs_files.json")
                            alcs_file_properties = ap.ALCSFileProperties(property_folder = alcs_property_folder, property_file = alcs_property_file)
                            alcs_file_properties_data = alcs_file_properties.get_alcs_file_properties()

                            alcs_files_list = alcs_file_properties_data.get("alcs_consolidated_data", '')
                            if alcs_files_list:
                                for alcs_file in alcs_files_list:
                                    alcs_file_split = af.ALCSConsolidatedFileSplit(
                                        data_frame=alcs_consolidated_data,
                                        bank_column='Bank Name',
                                        alcs_bank_name=alcs_file['alcs_bank_name'],
                                        alcs_file_path=alcs_file['alcs_file_path'],
                                        alcs_file_name=alcs_file['alcs_file_name']
                                    )

                                    store_file_output = alcs_file_split.store_alcs_file()
                                    if store_file_output:
                                        if file_uploads_all_properties:
                                            file_size = Path(store_file_output).stat().st_size
                                            payload_file_upload = json.dumps({
                                                "tenants_id": alcs_file_properties_data.get("tenants_id"),
                                                "groups_id": alcs_file_properties_data.get("groups_id"),
                                                "entities_id": alcs_file_properties_data.get("entity_id"),
                                                "m_source_id": alcs_file['m_source_id'],
                                                "m_processing_layer_id": alcs_file_properties_data.get("m_processing_layer_id"),
                                                "m_processing_sub_layer_id": alcs_file_properties_data.get("m_processing_sub_layer_id"),
                                                "processing_layer_id": alcs_file["processing_layer_id"],
                                                "processing_layer_name": alcs_file["processing_layer_name"],
                                                "source_type": alcs_file_properties_data.get("source_type"),
                                                "extraction_type": alcs_file_properties_data.get("extraction_type"),
                                                "file_name": store_file_output.split("/")[-1],
                                                "file_size_bytes": file_size,
                                                "file_upload_type": alcs_file_properties_data.get("file_upload_type"),
                                                "file_path": store_file_output,
                                                "status": alcs_file_properties_data.get("status"),
                                                "comments": alcs_file_properties_data.get("comments"),
                                                "is_processed": alcs_file_properties_data.get("is_processed"),
                                                "is_processing": alcs_file_properties_data.get("is_processing"),
                                                "is_active": alcs_file_properties_data.get("is_active"),
                                                "created_by": alcs_user_id,
                                                "created_date": alcs_upload_time,
                                                "modified_by": alcs_user_id
                                            })
                                            file_uploads_all_properties["data"] = payload_file_upload
                                            file_uploads = dr.PostResponse(file_uploads_all_properties)
                                            file_uploads_output = file_uploads.get_post_response_data()
                                            print("File Uploads output")
                                            print(file_uploads_output)
                                        else:
                                            print("Length of file_uploads_all_properties is equals to Zero!!!")
                                            break

                                file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                                if file_uploads_unique_record_properties:
                                    patch_payload = json.dumps({
                                        "status": "COMPLETED",
                                        "comments": "File Extracted Successfully!!!",
                                        "is_processed": 1
                                    })
                                    file_uploads_unique_record_properties_url_split = file_uploads_unique_record_properties["url"].split("/")
                                    file_uploads_unique_record_properties_url_split[-2] = str(alcs_file_id)
                                    file_uploads_unique_record_properties_url_proper = "/".join(file_uploads_unique_record_properties_url_split)
                                    file_uploads_unique_record_properties["url"] = file_uploads_unique_record_properties_url_proper
                                    file_uploads_unique_record_properties["data"] = patch_payload
                                    file_uploads_unique_record = dr.PatchResponse(file_uploads_unique_record_properties)
                                    file_uploads_unique_record_response = file_uploads_unique_record.get_patch_response()
                                    if file_uploads_unique_record_response["id"] == alcs_file_id:
                                        logging.info("Consolidated Files updated in DB!!!")
                                        logging.info(file_uploads_unique_record_response["id"])

                            else:
                                print("Length of ALCS Files List is equals to Zero!!!")
                                break
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
                                file_uploads_unique_record_properties_url_split = file_uploads_unique_record_properties["url"].split("/")
                                file_uploads_unique_record_properties_url_split[-2] = str(alcs_file_id)
                                file_uploads_unique_record_properties_url_proper = "/".join(file_uploads_unique_record_properties_url_split)
                                file_uploads_unique_record_properties["url"] = file_uploads_unique_record_properties_url_proper
                                file_uploads_unique_record_properties["data"] = patch_payload
                                file_uploads_unique_record = dr.PatchResponse(file_uploads_unique_record_properties)
                                file_uploads_unique_record_response = file_uploads_unique_record.get_patch_response()
                                if file_uploads_unique_record_response["id"] == alcs_file_id:
                                    logging.info("Error File Status Updated!!!")
                                    logging.info(file_uploads_unique_record_response["id"])

                    elif batch_files["file_path"].split("/")[-3] == 'BANK':
                        bank_file_id = batch_files["id"]
                        bank_user_id = batch_files["created_by"]
                        bank_upload_time = batch_files["created_date"]
                        bank_file_extract = bf.BankFileExtract(zip_file_path = batch_files["file_path"])
                        bank_file_extract_output = bank_file_extract.get_extract_bank_files()

                        if bank_file_extract_output["Status"] == "Success":
                            print("Bank File Extracted Successfully!!!")

                            bank_property_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
                            bank_property_file = os.path.join(bank_property_folder, "bank_files.json")
                            bank_file_properties = bp.BankFileProperties(property_folder=bank_property_folder, property_file=bank_property_file)
                            bank_file_properties_data = bank_file_properties.get_bank_file_properties()

                            folder_exists = False
                            for folder in os.listdir(bank_file_extract_output["path"]):
                                path = bank_file_extract_output["path"] + "/" + folder
                                # print("Path", path)
                                is_dir = os.path.isdir(path)
                                if is_dir:
                                    print("Folder Inside Zip File!!!")
                                    file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                                    get_update_file_upload_status(
                                        file_uploads_unique_record_properties=file_uploads_unique_record_properties,
                                        file_id=batch_files["id"],
                                        status='ERROR',
                                        comments='Folder Exists Inside Zip File!!!'
                                    )
                                    folder_exists = True
                                    break
                            # print("Folder Exists", folder_exists)
                            if folder_exists == False:
                                now = datetime.now().strftime("%d_%m_%Y")
                                for file in os.listdir(bank_file_extract_output["path"]):
                                    if file.split(".")[-1] in ["xls", "xlsx"]:
                                        file_without_extension = file.replace(file.split(".")[-1], "").replace(".", "").replace(" ", "_")
                                        proper_file_name = file_without_extension + "_" + now + "." + file.split(".")[-1]
                                        if re.search(r'axis', proper_file_name.lower()) and re.search(r'18294', proper_file_name.lower()):
                                            axis_properties = bank_file_properties_data.get("axis18294")
                                            shutil.copy(bank_file_extract_output["path"] + "/" + file, os.path.join(axis_properties["file_path"] + "/" + proper_file_name))

                                            if file_uploads_all_properties:
                                                file_size = Path(os.path.join(axis_properties["file_path"] + "/" + proper_file_name)).stat().st_size

                                                payload_file_upload = json.dumps({
                                                    "tenants_id": bank_file_properties_data.get("tenants_id"),
                                                    "groups_id": bank_file_properties_data.get("groups_id"),
                                                    "entities_id": bank_file_properties_data.get("entity_id"),
                                                    "m_source_id": axis_properties['m_source_id'],
                                                    "m_processing_layer_id": bank_file_properties_data.get("m_processing_layer_id"),
                                                    "m_processing_sub_layer_id": bank_file_properties_data.get("m_processing_sub_layer_id"),
                                                    "processing_layer_id": axis_properties["processing_layer_id"],
                                                    "processing_layer_name": axis_properties["processing_layer_name"],
                                                    "source_type": bank_file_properties_data.get("source_type"),
                                                    "extraction_type": bank_file_properties_data.get("extraction_type"),
                                                    "file_name": proper_file_name,
                                                    "file_size_bytes": file_size,
                                                    "file_upload_type": bank_file_properties_data.get("file_upload_type"),
                                                    "file_path": os.path.join(axis_properties["file_path"] + "/" + proper_file_name),
                                                    "status": bank_file_properties_data.get("status"),
                                                    "comments": bank_file_properties_data.get("comments"),
                                                    "is_processed": bank_file_properties_data.get("is_processed"),
                                                    "is_processing": bank_file_properties_data.get("is_processing"),
                                                    "is_active": bank_file_properties_data.get("is_active"),
                                                    "created_by": bank_user_id,
                                                    "created_date": bank_upload_time,
                                                    "modified_by": bank_user_id
                                                })
                                                file_uploads_all_properties["data"] = payload_file_upload
                                                file_uploads = dr.PostResponse(file_uploads_all_properties)
                                                file_uploads_output = file_uploads.get_post_response_data()
                                                print("File Uploads output")
                                                print(file_uploads_output)

                                            else:
                                                print("Length of file_uploads_all_properties is equals to Zero!!!")
                                                break

                                        elif re.search(r'icici', proper_file_name.lower()) and re.search(r'240', proper_file_name.lower()):
                                            icici_properties = bank_file_properties_data.get("icici240")
                                            shutil.copy(bank_file_extract_output["path"] + "/" + file, os.path.join(icici_properties["file_path"] + "/" + proper_file_name))

                                            if file_uploads_all_properties:
                                                file_size = Path(os.path.join(icici_properties["file_path"] + "/" + proper_file_name)).stat().st_size

                                                payload_file_upload = json.dumps({
                                                    "tenants_id": bank_file_properties_data.get("tenants_id"),
                                                    "groups_id": bank_file_properties_data.get("groups_id"),
                                                    "entities_id": bank_file_properties_data.get("entity_id"),
                                                    "m_source_id": icici_properties['m_source_id'],
                                                    "m_processing_layer_id": bank_file_properties_data.get("m_processing_layer_id"),
                                                    "m_processing_sub_layer_id": bank_file_properties_data.get("m_processing_sub_layer_id"),
                                                    "processing_layer_id": icici_properties["processing_layer_id"],
                                                    "processing_layer_name": icici_properties["processing_layer_name"],
                                                    "source_type": bank_file_properties_data.get("source_type"),
                                                    "extraction_type": bank_file_properties_data.get("extraction_type"),
                                                    "file_name": proper_file_name,
                                                    "file_size_bytes": file_size,
                                                    "file_upload_type": bank_file_properties_data.get("file_upload_type"),
                                                    "file_path": os.path.join(icici_properties["file_path"] + "/" + proper_file_name),
                                                    "status": bank_file_properties_data.get("status"),
                                                    "comments": bank_file_properties_data.get("comments"),
                                                    "is_processed": bank_file_properties_data.get("is_processed"),
                                                    "is_processing": bank_file_properties_data.get("is_processing"),
                                                    "is_active": bank_file_properties_data.get("is_active"),
                                                    "created_by": bank_user_id,
                                                    "created_date": bank_upload_time,
                                                    "modified_by": bank_user_id
                                                })
                                                file_uploads_all_properties["data"] = payload_file_upload
                                                file_uploads = dr.PostResponse(file_uploads_all_properties)
                                                file_uploads_output = file_uploads.get_post_response_data()
                                                print("File Uploads output")
                                                print(file_uploads_output)

                                            else:
                                                print("Length of file_uploads_all_properties is equals to Zero!!!")
                                                break

                                        elif re.search(r'sbi', proper_file_name.lower()) and re.search(r'913', proper_file_name.lower()):
                                            sbi_properties = bank_file_properties_data.get("sbi913")
                                            shutil.copy(bank_file_extract_output["path"] + "/" + file, os.path.join(sbi_properties["file_path"] + "/" + proper_file_name))

                                            if file_uploads_all_properties:
                                                file_size = Path(os.path.join(sbi_properties["file_path"] + "/" + proper_file_name)).stat().st_size

                                                payload_file_upload = json.dumps({
                                                    "tenants_id": bank_file_properties_data.get("tenants_id"),
                                                    "groups_id": bank_file_properties_data.get("groups_id"),
                                                    "entities_id": bank_file_properties_data.get("entity_id"),
                                                    "m_source_id": sbi_properties['m_source_id'],
                                                    "m_processing_layer_id": bank_file_properties_data.get("m_processing_layer_id"),
                                                    "m_processing_sub_layer_id": bank_file_properties_data.get("m_processing_sub_layer_id"),
                                                    "processing_layer_id": sbi_properties["processing_layer_id"],
                                                    "processing_layer_name": sbi_properties["processing_layer_name"],
                                                    "source_type": bank_file_properties_data.get("source_type"),
                                                    "extraction_type": bank_file_properties_data.get("extraction_type"),
                                                    "file_name": proper_file_name,
                                                    "file_size_bytes": file_size,
                                                    "file_upload_type": bank_file_properties_data.get("file_upload_type"),
                                                    "file_path": os.path.join(sbi_properties["file_path"] + "/" + proper_file_name),
                                                    "status": bank_file_properties_data.get("status"),
                                                    "comments": bank_file_properties_data.get("comments"),
                                                    "is_processed": bank_file_properties_data.get("is_processed"),
                                                    "is_processing": bank_file_properties_data.get("is_processing"),
                                                    "is_active": bank_file_properties_data.get("is_active"),
                                                    "created_by": bank_user_id,
                                                    "created_date": bank_upload_time,
                                                    "modified_by": bank_user_id
                                                })
                                                file_uploads_all_properties["data"] = payload_file_upload
                                                file_uploads = dr.PostResponse(file_uploads_all_properties)
                                                file_uploads_output = file_uploads.get_post_response_data()
                                                print("File Uploads output")
                                                print(file_uploads_output)

                                            else:
                                                print("Length of file_uploads_all_properties is equals to Zero!!!")
                                                break

                                        elif re.search(r'hdfc', proper_file_name.lower()) and re.search(r'062', proper_file_name.lower()):
                                            hdfc_properties = bank_file_properties_data.get("hdfc062")
                                            shutil.copy(bank_file_extract_output["path"] + "/" + file, os.path.join(hdfc_properties["file_path"] + "/" + proper_file_name))

                                            if file_uploads_all_properties:
                                                file_size = Path(os.path.join(hdfc_properties["file_path"] + "/" + proper_file_name)).stat().st_size

                                                payload_file_upload = json.dumps({
                                                    "tenants_id": bank_file_properties_data.get("tenants_id"),
                                                    "groups_id": bank_file_properties_data.get("groups_id"),
                                                    "entities_id": bank_file_properties_data.get("entity_id"),
                                                    "m_source_id": hdfc_properties['m_source_id'],
                                                    "m_processing_layer_id": bank_file_properties_data.get("m_processing_layer_id"),
                                                    "m_processing_sub_layer_id": bank_file_properties_data.get("m_processing_sub_layer_id"),
                                                    "processing_layer_id": hdfc_properties["processing_layer_id"],
                                                    "processing_layer_name": hdfc_properties["processing_layer_name"],
                                                    "source_type": bank_file_properties_data.get("source_type"),
                                                    "extraction_type": bank_file_properties_data.get("extraction_type"),
                                                    "file_name": proper_file_name,
                                                    "file_size_bytes": file_size,
                                                    "file_upload_type": bank_file_properties_data.get("file_upload_type"),
                                                    "file_path": os.path.join(hdfc_properties["file_path"] + "/" + proper_file_name),
                                                    "status": bank_file_properties_data.get("status"),
                                                    "comments": bank_file_properties_data.get("comments"),
                                                    "is_processed": bank_file_properties_data.get("is_processed"),
                                                    "is_processing": bank_file_properties_data.get("is_processing"),
                                                    "is_active": bank_file_properties_data.get("is_active"),
                                                    "created_by": bank_user_id,
                                                    "created_date": bank_upload_time,
                                                    "modified_by": bank_user_id
                                                })
                                                file_uploads_all_properties["data"] = payload_file_upload
                                                file_uploads = dr.PostResponse(file_uploads_all_properties)
                                                file_uploads_output = file_uploads.get_post_response_data()
                                                print("File Uploads output")
                                                print(file_uploads_output)

                                            else:
                                                print("Length of file_uploads_all_properties is equals to Zero!!!")
                                                break

                                            hdfc_neft_properties = bank_file_properties_data.get("hdfc_neft")
                                            shutil.copy(bank_file_extract_output["path"] + "/" + file, os.path.join(hdfc_neft_properties["file_path"] + "/" + proper_file_name))

                                            if file_uploads_all_properties:
                                                file_size = Path(os.path.join(hdfc_neft_properties["file_path"] + "/" + proper_file_name)).stat().st_size

                                                payload_file_upload = json.dumps({
                                                    "tenants_id": bank_file_properties_data.get("tenants_id"),
                                                    "groups_id": bank_file_properties_data.get("groups_id"),
                                                    "entities_id": bank_file_properties_data.get("entity_id"),
                                                    "m_source_id": hdfc_neft_properties['m_source_id'],
                                                    "m_processing_layer_id": bank_file_properties_data.get("m_processing_layer_id"),
                                                    "m_processing_sub_layer_id": bank_file_properties_data.get("m_processing_sub_layer_id"),
                                                    "processing_layer_id": hdfc_neft_properties["processing_layer_id"],
                                                    "processing_layer_name": hdfc_neft_properties["processing_layer_name"],
                                                    "source_type": bank_file_properties_data.get("source_type"),
                                                    "extraction_type": bank_file_properties_data.get("extraction_type"),
                                                    "file_name": proper_file_name,
                                                    "file_size_bytes": file_size,
                                                    "file_upload_type": bank_file_properties_data.get("file_upload_type"),
                                                    "file_path": os.path.join(hdfc_neft_properties["file_path"] + "/" + proper_file_name),
                                                    "status": bank_file_properties_data.get("status"),
                                                    "comments": bank_file_properties_data.get("comments"),
                                                    "is_processed": bank_file_properties_data.get("is_processed"),
                                                    "is_processing": bank_file_properties_data.get("is_processing"),
                                                    "is_active": bank_file_properties_data.get("is_active"),
                                                    "created_by": bank_user_id,
                                                    "created_date": bank_upload_time,
                                                    "modified_by": bank_user_id
                                                })
                                                file_uploads_all_properties["data"] = payload_file_upload
                                                file_uploads = dr.PostResponse(file_uploads_all_properties)
                                                file_uploads_output = file_uploads.get_post_response_data()
                                                print("File Uploads output")
                                                print(file_uploads_output)

                                            else:
                                                print("Length of file_uploads_all_properties is equals to Zero!!!")
                                                break

                            file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                            if file_uploads_unique_record_properties:
                                patch_payload = json.dumps({
                                    "status": "COMPLETED",
                                    "comments": "File Extracted Successfully!!!",
                                    "is_processed": 1
                                })
                                file_uploads_unique_record_properties_url_split = file_uploads_unique_record_properties["url"].split("/")
                                file_uploads_unique_record_properties_url_split[-2] = str(bank_file_id)
                                file_uploads_unique_record_properties_url_proper = "/".join(file_uploads_unique_record_properties_url_split)
                                file_uploads_unique_record_properties["url"] = file_uploads_unique_record_properties_url_proper
                                file_uploads_unique_record_properties["data"] = patch_payload
                                file_uploads_unique_record = dr.PatchResponse(file_uploads_unique_record_properties)
                                file_uploads_unique_record_response = file_uploads_unique_record.get_patch_response()
                                if file_uploads_unique_record_response["id"] == bank_file_id:
                                    logging.info("BANK RAR Files updated in DB!!!")
                                    logging.info(file_uploads_unique_record_response["id"])

                            shutil.rmtree(bank_file_extract_output["path"])

                        elif bank_file_extract_output["Status"] == "Error":
                            logging.info("Error in Bank File Extract Output!!!")
                            file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                            if file_uploads_unique_record_properties:
                                get_update_file_upload_status(
                                    file_uploads_unique_record_properties = file_uploads_unique_record_properties,
                                    file_id = batch_files["id"],
                                    status = 'ERROR',
                                    comments = 'Error in Extract Files!!!'
                                )
                                break
                        elif bank_file_extract_output["Status"] == "Exists":
                            logging.info("Bank Extract Folder Already Exists!!!")
                            file_uploads_unique_record_properties = api_properties_data.get("file_uploads_unique_record_properties", "")
                            if file_uploads_unique_record_properties:
                                get_update_file_upload_status(
                                    file_uploads_unique_record_properties=file_uploads_unique_record_properties,
                                    file_id=batch_files["id"],
                                    status='EXISTS',
                                    comments='Error in Extract Files!!!'
                                )
                                break
            else:
                print("No Files Exists in Batch ALL!!!")
    except Exception as e:
        print(e)
        logging.error("Error in ALCS File Upload Auto File!!!", exc_info=True)
