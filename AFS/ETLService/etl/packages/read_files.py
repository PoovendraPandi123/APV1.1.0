import pandas as pd
import numpy as np
from datetime import datetime

month_list = ["January", "Jan", "February", "Feb", "March", "Mar", "April", "Apr","May","May","June","Jun","July","Jul","August","Aug","September","Sep","October","Oct","November","Nov","December","Dec"]

month_values = {
    "January"       : "01",
    "Jan"           : "01",
    "February"      : "02",
    "Feb"           : "02",
    "March"         : "03",
    "Mar"           : "03",
    "April"         : "04",
    "Apr"           : "04",
    "May"           : "05",
    "June"          : "06",
    "Jun"           : "06",
    "July"          : "07",
    "Jul"           : "07",
    "August"        : "08",
    "Aug"           : "08",
    "September"     : "09",
    "Sep"           : "09",
    "October"       : "10",
    "Oct"           : "10",
    "November"      : "11",
    "Nov"           : "11",
    "December"      : "12",
    "Dec"           : "12"
}

def get_read_file(file_path, source_definitions):
    try:
        # TODO : skiprows to be dynamic
        # Getting the Column Names from the definitions
        # print(source_definitions)
        attribute_names = []
        attribute_positions = []
        attribute_reference_fields = []
        attribute_reference_table = []
        for i in range(0, len(source_definitions)):
            if source_definitions[i]["attribute_position"] == i + 2:
                attribute_names.append(source_definitions[i]["attribute_name"])
                attribute_positions.append(source_definitions[i]["attribute_position"])
                attribute_reference_fields.append(source_definitions[i]["attribute_reference_field"])
                attribute_reference_table.append(source_definitions[i]["attribute_reference_table"])
        # print(attribute_names)
        # print(len(attribute_names))

        # Making all the column names to string
        data_column_converter = {}
        for name in attribute_names:
            data_column_converter[name] =  str

        # Reading the file with all the columns as string
        # For CSV
        if str(file_path).split(".")[-1] == "csv":
            data = pd.read_csv(file_path, converters=data_column_converter, skiprows=1, usecols=attribute_names)
        # For Excel
        if str(file_path).split(".")[-1] in ["xlsx", "xls"]:
            data = pd.read_excel(file_path, skiprows=1)

        # store_file = file_path.split(".")[-1]
        # file_name_replaced = file_path.replace(store_file, "_converted.xlsx")
        #
        # # Storing the File
        # data.to_csv(file_name_replaced, index=False)
        #
        # # Reading the file again with required columns only
        # data = pd.read_csv(file_name_replaced, usecols=attribute_names, converters=data_column_converter)

        # print("Data")
        # print(data.columns)
        # print(len(data.columns))
        # checking the column names
        # data_columns = data.columns
        # column_check = 1
        # for i in range(1, len(data_columns)):
        #     if data_columns[i] == attribute_names[i-1]:
        #         column_check += 1
        # print(attribute_reference_table)
        if len(list(set(attribute_reference_table))) == 1:
            attribute_table = list(set(attribute_reference_table))[0]
            # if column_check == len(data_columns):
                # Removing the spaces, tab and new lines in the text for all columns
            # for i in attribute_positions:
            for i in range(0, len(data.columns)):
                # proper_index = i - 1
                proper_index = i
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.lstrip()
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.rstrip()
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace('\t', '')
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace('\n', '')
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace("'", "(())")

            data_proper = data.replace(np.nan, '')
            # data_proper.drop(data_proper.columns[0], axis='columns', inplace=True) # Removing the Sl.No Column

            output = {
                "Status": "Success",
                "data": data_proper,
                "data_columns": data_proper.columns,
                "attribute_reference_fields": attribute_reference_fields,
                "attribute_table" : attribute_table
            }
            return output
            # else:
            #     return {"Stats" : "Error", "message" : "Column Names Not Matched in the file " + file_path}
        else:
            return {"Status": "Error", "message": "Table Names are Varied!!!"}

    except Exception as e:
        return {"Status" : "Error", "message" : str(e)}


#def convert_date(date_string):
    #try:
        #if len(str(date_string)) > 1:
            #excel_date = int(date_string)
            #dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
            # tt = dt.timetuple()
            #return str(dt)
        #elif len(str(date_string)) < 1:
            #return date_string
    #except Exception:
        #return date_string

def convert_format(date_string):
    try:
        date_input = str(date_string)
        year = date_input.split("-")[2]
        month = date_input.split("-")[1]
        day = date_input.split("-")[0]

        if month in month_list:
            if len(year) == 2:
                year = "20" + year
            if len(day) == 1:
                day = "0" + day
            month_value = month_values[month]
            output_date = year + "-" + month_value + "-" + day + " 00:00:00"
            return output_date
        else:
            return date_string
    except Exception:
        return date_string

def convert_date(date_string):
    try:
        if len(str(date_string)) > 1:
            excel_date = int(date_string)
            dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
            # tt = dt.timetuple()
            return str(dt)
        elif len(str(date_string)) < 1:
            return date_string
    except Exception:
        # print("Hai")
        # convert_format(date_string)
        return convert_format(date_string)

def get_data_from_file(file_path, sheet_name, source_extension, attribute_list, column_start_row, password_protected, source_password, attribute_data_types_list):
    try:

        # Making all the column names to string
        data_column_converter = {}
        for name in attribute_list:
            data_column_converter[name] = str

        data = ''
        if source_extension in ["csv"]:
            data = pd.read_csv(file_path, skiprows=int(column_start_row) - 1, usecols = attribute_list, converters = data_column_converter)
        elif source_extension in ["xlsx", "xls"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1, sheet_name = sheet_name, usecols = attribute_list, converters = data_column_converter)
        elif source_extension in ["xlsb"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1, sheet_name = sheet_name, usecols = attribute_list, engine="pyxlsb", converters = data_column_converter)

        if len(data) > 0:
            for i in range(0, len(data.columns)):
                proper_index = i
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.lstrip()
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.rstrip()
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace('\t', '')
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace('\n', '')
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace("'", "/#/")
                data[data.columns[proper_index]] = data[data.columns[proper_index]].str.replace("\\", "/##/")

            data_proper = data.replace(np.nan, '')

            for j in range(0, len(attribute_data_types_list)):
                if attribute_data_types_list[j] == "date":
                    data_proper[attribute_list[j]] = data_proper[attribute_list[j]].apply(convert_date)

            data_output = {
                "data" : data_proper
            }

            return {"Status" : "Success", "data" : data_output}
        else:
            return {"Status" : "Error", "Message" : "Length of Data is Less than 0!!!"}
    except Exception as e:
        return {"Status" : "Error", "Message" : str(e)}


def read_file_for_source(file_path, column_start_row, source_extension):
    try:
        if source_extension in [".csv"]:
            data = pd.read_csv(file_path, skiprows=int(column_start_row) - 1)
        elif source_extension in [".xlsx", ".xls"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1)
        elif source_extension in [".xlsb"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1, engine="pyxlsb")

        data_headers = list(data.columns)

        return {"Status" : "Success", "data_header" : data_headers}

    except Exception as e:
        return {"Status" : "Error", "Message" : str(e)}