import pandas as pd
import numpy as np

def validate_file(file_path, source_config):
    try:
        source_configurations = source_config
        for k,v in source_configurations.items():
            if k == "sheet_name":
                sheet_name = v
            if k == "column_start_row":
                column_start_row = v
            if k == "source_extension":
                source_extension = v

        if str(file_path).split(".")[-1] == source_extension.replace(".", ""):
            # For CSV
            if str(file_path).split(".")[-1] == "csv":
                data = pd.read_csv(file_path, skiprows=int(column_start_row) - 1)
            # For Excel
            if str(file_path).split(".")[-1] in ["xlsx", "xls"]:
                data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1)
            # For Excel Binary
            if str(file_path).split(".")[-1] in ["xlsb"]:
                data = pd.read_excel(file_path, engine='pyxlsb', skiprows=int(column_start_row) - 1)

            rows_count = len(data)

            data = {
                "rows_count" : rows_count
            }

            return {"Status" : "Success", "data" : data}
        else:
            return {"Status" : "User Error", "Message" : "Source Extension is not Matching with the source definitions!!!"}
    except Exception as e:
        return {"Status" : "Error", "message" : str(e)}