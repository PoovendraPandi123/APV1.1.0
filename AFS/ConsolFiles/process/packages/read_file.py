import pandas as pd
import numpy as np
from datetime import datetime
import logging
import re

logger = logging.getLogger("consolidation_files")

month_list = ["january", "jan", "february", "feb", "march", "mar", "april", "apr", "may", "june", "jun", "july",
		"jul", "august", "aug", "september", "sep", "october", "oct", "november", "nov", "december", "dec"]

month_values = {
    "january": "01",
    "jan": "01",
    "february": "02",
    "feb": "02",
    "march": "03",
    "mar": "03",
    "april": "04",
    "apr": "04",
    "may": "05",
    "june": "06",
    "jun": "06",
    "july": "07",
    "jul": "07",
    "august": "08",
    "aug": "08",
    "september": "09",
    "sep": "09",
    "october": "10",
    "oct": "10",
    "november": "11",
    "nov": "11",
    "december": "12",
    "dec": "12"
}

def convert_format(date_string):
    try:
        date_input = str(date_string)
        # print(date_input)
        month_hiffen = ''
        year_hiffen = ''
        day_hiffen = ''
        year_slash = ''
        month_slash = ''
        day_slash = ''
        year_hiffen_time = ''
        month_hiffen_time = ''
        day_hiffen_tme = ''
        time_and_second = " 00:00:00"

        if re.search('[a-z][A-Z]', date_input):
            date_input_proper = date_input.lower()
            date_value = date_input_proper
            year_hiffen = date_value.split("-")[2]
            month_hiffen = date_value.split("-")[0]
            day_hiffen = date_value.split("-")[1]

        if re.search("-", date_input) and ( re.search("AM", date_input) or re.search("PM", date_input) ):
            date_input_proper = date_input.replace(" AM", "").replace(" PM", "")
            time_and_second = date_input_proper.split(" ")[1]
            date_value = date_input_proper.split(" ")[0]
            year_hiffen_time = date_value.split("-")[2]
            month_hiffen_time = date_value.split("-")[0]
            day_hiffen_tme = date_value.split("-")[1]

        elif re.search("/", date_input) and ( re.search("AM", date_input) or re.search("PM", date_input) ) and re.search("yes", date_key_word_var.lower()):
            date_input_proper = date_input.replace(" AM", "").replace(" PM", "")
            time_and_second = date_input_proper.split(" ")[1]
            date_value = date_input_proper.split(" ")[0]
            year_hiffen_time = date_value.split("/")[2]
            month_hiffen_time = date_value.split("/")[0]
            day_hiffen_tme = date_value.split("/")[1]

        elif re.search("/", date_input) and ( re.search("AM", date_input) or re.search("PM", date_input) ):
            date_input_proper = date_input.replace(" AM", "").replace(" PM", "")
            time_and_second = date_input_proper.split(" ")[1]
            date_value = date_input_proper.split(" ")[0]
            year_hiffen_time = date_value.split("/")[2]
            month_hiffen_time = date_value.split("/")[1]
            day_hiffen_tme = date_value.split("/")[0]

        elif re.search("MAN_MACHINE_GL", date_key_word_var) and re.search(".", date_input):
            date_value = date_input
            year_hiffen_time = date_value.split(".")[2]
            month_hiffen_time = date_value.split(".")[1]
            day_hiffen_tme = date_value.split(".")[0]

        elif re.search("MS_PUSHPAK_STEEL_GL", date_key_word_var) and re.search(".", date_input):
            date_value = date_input
            year_hiffen_time = date_value.split(".")[2]
            month_hiffen_time = date_value.split(".")[1]
            day_hiffen_tme = date_value.split(".")[0]

        elif re.search("-", date_input) and len(date_input.split("-")[0]) == 4:
            date_input_proper = date_input.split(" ")[0]
            year_hiffen = date_input_proper.split("-")[0]
            month_hiffen = date_input_proper.split("-")[1]
            day_hiffen = date_input_proper.split("-")[2]

        elif re.search("-", date_input):
            year_hiffen = date_input.split("-")[2]
            month_hiffen = date_input.split("-")[1]
            day_hiffen = date_input.split("-")[0]

        elif re.search("/", date_input):
            year_slash = date_input.split("/")[2]
            month_slash = date_input.split("/")[1]
            day_slash = date_input.split("/")[0]

        if month_slash in month_list:
            # print("Eighth")
            year = year_slash
            day = day_slash
            if len(day_slash) == 1:
                day = "0" + day_hiffen
            month_value = month_values[month_slash]
            output_date = year + "-" + month_value + "-" + day + time_and_second
            # print("output_date", output_date)
            return output_date

        if month_hiffen in month_list:
            year = year_hiffen
            day = day_hiffen
            if len(year_hiffen) == 2:
                year = "20" + year_hiffen
            if len(day_hiffen) == 1:
                day = "0" + day_hiffen
            month_value = month_values[month_hiffen]
            output_date = year + "-" + month_value + "-" + day + time_and_second
            return output_date

        elif len(year_hiffen) > 0 and len(month_hiffen) > 0 and len(day_hiffen) > 0:
            # print(date_input)
            # print(year_hiffen)
            # print(month_hiffen)
            # print(day_hiffen)
            year = year_hiffen
            month = month_hiffen
            day = day_hiffen
            if len(year_hiffen) == 2:
                year = "20" + year_hiffen
            if len(day_hiffen) == 1:
                day = "0" + day_hiffen
            if len(month_hiffen) == 1:
                month = "0" + month_hiffen
            output_date = year + "-" + month + "-" + day + time_and_second
            return output_date

        elif len(year_slash) > 0 and len(month_slash) > 0 and len(day_slash) > 0:
            year = year_slash
            month = month_slash
            day = day_slash
            if len(year_slash) == 2:
                year = "20" + year_slash
            if len(day_slash) == 1:
                day = "0" + day_slash
            if len(month_slash) == 1:
                month = "0" + month_slash
            output_date = year + "-" + month + "-" + day + time_and_second
            return output_date

        elif len(year_hiffen_time) > 0 and len(month_hiffen_time) > 0 and len(day_hiffen_tme) > 0:
            year = year_hiffen_time
            month = month_hiffen_time
            day = day_hiffen_tme
            if len(year_hiffen_time) == 2:
                year = "20" + year_hiffen_time
            if len(day_hiffen_tme) == 1:
                day = "0" + day_hiffen_tme
            if len(month_hiffen_time) == 1:
                month = "0" + month_hiffen_time
            output_date = year + "-" + month + "-" + day + " " + time_and_second
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
        return convert_format(date_string)

def get_data_from_file(file_path, sheet_name, source_extension, attribute_list, column_start_row, password_protected,
                       source_password, attribute_data_types_list, unique_list, primary_date_list, date_key_word):
    try:

        global date_key_word_var

        date_key_word_var = date_key_word

        # Making all the column names to string
        data_column_converter = {}
        for name in attribute_list:
            data_column_converter[name] = str

        data = ''

        if source_extension in ["csv"]:
            data = pd.read_csv(file_path, skiprows=int(column_start_row) - 1, usecols=attribute_list, converters=data_column_converter)[attribute_list]
        elif source_extension in ["xlsx", "xls"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1, usecols=attribute_list, converters=data_column_converter)[attribute_list]
        elif source_extension in ["xlsb"]:
            data = pd.read_excel(file_path, skiprows=int(column_start_row) - 1, usecols=attribute_list, engine="pyxlsb", converters=data_column_converter)[attribute_list]

        if len(data) > 0:

            data_proper = pd.DataFrame()
            unique_column = ''
            date_primary_column = ''

            data_proper = data.replace(np.nan, '')

            # print("Data Removed NAN")
            # print(data_proper)

            # print("unique_list", unique_list)

            for k in range(0, len(unique_list)):
               if int(unique_list[k]) == 1:
                    unique_column = attribute_list[k]

            for z in range(0, len(primary_date_list)):
                if int(primary_date_list[z]) == 1:
                    date_primary_column = attribute_list[z]

            # print("unique_column", unique_column)

            data_proper["primary_date_length"] = data_proper[date_primary_column].apply(len)
            data_proper["data_length"] = data_proper[unique_column].apply(len)
            # print("data_proper_first")
            # print(data_proper)
            data_filter = data_proper[data_proper["data_length"] > 2]
            # print("data_proper_second")
            # print(data_proper)
            data_filter.drop("data_length", axis=1, inplace=True)

            data_primary_date_filter = data_filter[data_filter["primary_date_length"] > 2]
            data_primary_date_filter.drop("primary_date_length", axis=1, inplace=True)
            #
            # print("Length Check for Unique")
            # print(data_filter)

            for i in range(0, len(data_primary_date_filter.columns)):
                proper_index = i
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.lstrip()
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.rstrip()
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.replace('\t', '')
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.replace('\n', '')
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.replace("'", "/#/")
                data_primary_date_filter[data_primary_date_filter.columns[proper_index]] = data_primary_date_filter[data_primary_date_filter.columns[proper_index]].str.replace("\\", "/##/")


            # print("Removed Unncessaries")
            # print(data_filter)

            for j in range(0, len(attribute_data_types_list)):
                if attribute_data_types_list[j] == "date":
                    data_filter[attribute_list[j]] = data_filter[attribute_list[j]].apply(convert_date)

            # print("Date Converted")
            # print(data_filter)

            data_output = {
                "data": data_filter
            }
            return {"Status": "Success", "data": data_output}
        else:
            logger.info("Length of the data is less than 0 in get data from file!!!", exc_info=True)
            return {"Status": "DataLength", "Message": "Length of Data is Less than 0!!!"}
    except Exception as e:
        logger.error("Error in Reading Data in get data from file!!!", exc_info=True)
        return {"Status": "Error", "Message": str(e)}