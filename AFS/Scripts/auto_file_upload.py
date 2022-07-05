import os
import re
import shutil
from AFS.Scripts.sample_request import get_execute_etl, post_execute_file_upload
from datetime import datetime
import xlrd as xl


def tool():
    try:
        input_path = "D:\\Advents\\Cosnolidation Files\\Data\\Consolidated Sales Register_Workings Mar-21\\"
        files = os.listdir(input_path)
        if bool(files) == False:
            print("No files Available")
        else:
            get_all_files_from(files)

        return {"Status": "Success"}
    except Exception as e:
        print(e)
        return {"Status": "Error"}


def get_all_files_from(files):
    try:
        print(files)
        source_details = get_execute_etl()
        source_key = []
        for get_key in source_details['data']:
            source_key.append(
                {
                    "source_id": get_key['id'],
                    "source_name": get_key['source_name'],
                    "source_key_value": get_key['source_config']['source_key'],
                    "source_def": get_key['source_definitions']
                }
            )
        print(source_key)
        for file_name in files:
            source_dict = []
            for key in source_key:
                flag = 0
                len_key = len(key['source_key_value'])
                for i in key['source_key_value']:
                    file_lower = file_name.lower()
                    if bool(re.search(i, file_lower)):
                        print(i)
                        flag = flag + 1
                        if flag == len_key:
                            print("inside break")
                            break
                if flag == len_key:
                    source_dict.append(
                        {
                            "source_id": key['source_id'],
                            "source_key_value": key['source_key_value'],
                            "source_name": key['source_name'],
                            "source_def": key['source_def'],
                            "file_name": file_name
                        }
                    )
                    break
                print(source_dict)
            # file_upload_execute(source_dict)
            file_validation(source_dict)
        return {"Status": "Success"}
    except Exception as e:
        print(e)
        return {"Status": "Error"}


def file_upload_execute(source_dict):
    try:
        for dict in source_dict:
            data = {
                "id": "",
                "tenants_id": "1",
                "groups_id": "1",
                "entities_id": "1",
                "module_id": "2",
                "source_name": dict['source_name'],
                "source_type": "file",
                "extraction_type": "upload",
                "file_name": dict['file_name'],
                "file_size_bytes": "",
                "status": "Validated",
                "comments": "Validated",
                "file_row_count": "",
                "is_processed": 0,
                "file_path": "",
                "is_active": True,
                "created_by": 0,
                "created_date": str(datetime.today()),
                "modified_by": 0,
                "modified_date": str(datetime.today()),
                "m_sources": dict['source_id']
            }
            upload_detail = post_execute_file_upload(data)

        return {"Status": "Success"}
    except Exception as e:
        print(e)
        return {"Status": "Error"}


def file_validation(source_dict):
    try:
        print(source_dict)
        for dict in source_dict:
            file_name = dict['file_name']
            input_path = "D:\\Advents\\Cosnolidation Files\\Data\\Consolidated Sales Register_Workings Mar-21\\"
            source = (input_path + file_name)
            wb = xl.open_workbook(source)  # opening & reading the excel fil
            s1 = wb.sheet_by_index(0)  # extracting the worksheet
            col = s1.ncols
            row = s1.nrows
            print("No. of rows:", row)
            print("No. of columns:", col)
            col_def_len = len(dict['source_def'])
            source_def = dict['source_def']
            if col == col_def_len:
                print("no of colm validated")
                for i in range(0, col):
                    for col_def in source_def:
                        data_type = col_def['attribute_data_type']
                        pos = col_def['attribute_reference_field']
                        col_pos = i + 1
                        if int(pos) == col_pos:
                            print("position matched")  # initializing cell from the excel file mentioned through the
                            # cell position
                            for j in range(1, row):
                                data = s1.cell_value(j, i)
                                data_str = str(data)
                                print(data)
                                if data_type == "Integer":
                                    if re.match(r'^-?\d+(?:\.\d+)$', data_str) is None:
                                        print("Not float")
                                    else:
                                        print("Float Value")
                            # print(len(data))

        return {"Status": "Success"}
    except Exception as e:
        print(e)
        return {"Status": "Error"}


def move_file(input_location, file_name):
    try:
        input_path = "D:\\Advents\\Cosnolidation Files\\Data\\Consolidated Sales Register_Workings Mar-21\\"
        output_path = "D:\\AdventsProduct\\V1.1.0\\AFS\\ConsolFiles\\Data\\"
        # fetch all files
        for file_name in os.listdir(input_path):
            # construct full file path
            print("files:", file_name)
            source = input_path + file_name
            destination = output_path + file_name
            if not os.path.exists(destination):
                os.mkdir(destination)
            source_path_proper_input = destination + "\\" + "input\\"
            if not os.path.exists(source_path_proper_input):
                os.mkdir(source_path_proper_input)
            # move files
            print("moving file")
            shutil.move(source, source_path_proper_input)
            print('Moved:', file_name)
        return {"Status": "Success"}
    except Exception as e:
        print(e)
        return {"Status": "Error"}


if __name__ == "__main__":
    tool()
