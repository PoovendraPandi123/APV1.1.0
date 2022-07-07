import xlrd as xl
import re
import logging
import os
import pandas as pd

logger = logging.getLogger("consolidation_files")

class FileValidation:

    _file_name = ''
    _source_dict = ''
    _source_def_dict = ''
    _file_exists = False
    _keyword_check = False
    _column_count = False
    _data_type_check = False
    _data = pd.DataFrame()
    _data_columns = []
    _source_def_attribute_name_list = []
    _mismatch_position = -1
    _mismatch_data_list = []
    _position_check = False
    _unmatched_columns_list_source_def = []
    _unmatched_columns_list_data = []
    _column_start_row = 0
    _incorrect_data_type_list_data = []
    _excel_data = pd.DataFrame()

    def __init__(self, file, source_dict, source_def_dict):
        self._file_name = file
        self._source_dict = source_dict
        self._source_def_dict = source_def_dict
        self.check_file_exists()
        self.check_key_words()
        self.check_column_position()
        self.check_data_type()

    def check_file_exists(self):
        try:
            if os.path.exists(self._file_name):
                self._file_exists = True
        except Exception:
            logger.error("Error in File Exists Function of File Validation Class!!!", exc_info=True)

    def check_key_words(self):
        try:
            keywords_dict = self._source_dict["key_words"]["keywords"]
            # print("keywords_dict")
            flag = 0
            len_key = len(keywords_dict)
            for keyword in keywords_dict:
                file_lower = self._file_name.split("/")[-1].lower()
                # print("file_lower", file_lower)
                if bool(re.search(keyword, file_lower)):
                    # print(i)
                    flag = flag + 1
            if flag == len_key:
                self._keyword_check = True

        except Exception:
            logger.error("Error in Check Key Words Function of File Validation Class!!!", exc_info=True)


    def check_column_position(self):
        try:

            self._column_start_row = self._source_dict["source_config"]["column_start_row"]

            for source_def in self._source_def_dict:
                self._source_def_attribute_name_list.append(source_def["attribute_name"])

            if self._file_name.split(".")[-1] == "xlsb":
                self._data = pd.read_excel(self._file_name, engine="pyxlsb", skiprows=int(self._column_start_row)-1)
            elif self._file_name.split(".")[-1] == "xlsx" or self._file_name.split(".")[-1] == "xls":
                self._data = pd.read_excel(self._file_name, skiprows=int(self._column_start_row)-1)

            self._data_columns = list(self._data.columns)

            if len(self._source_def_attribute_name_list) == len(self._data_columns):
                self._column_count = True

            # print("attribute name list", self._source_def_attribute_name_list)
            # print("data columns", self._data_columns)

            # 1) Checking Column Name Match and Position File
            if self._column_count == True:
                for i in range(0, len(self._source_def_attribute_name_list)):
                    if self._source_def_attribute_name_list[i] == self._data_columns[i]:
                        continue
                    else:
                        self._mismatch_position = i
                        self._mismatch_data_list.append({
                            "position": self._mismatch_position + 1,
                            "source_def_attr_name": self._source_def_attribute_name_list[self._mismatch_position],
                            "data_col_name": self._data_columns[self._mismatch_position]
                        })

                if self._mismatch_position == -1:
                    self._position_check = True

            # 2) Check the Extra Columns in File
            elif self._column_count == False:
                set_source_def_attribute_name_list = set(self._source_def_attribute_name_list)
                set_data_columns = set(self._data_columns)
                self._unmatched_columns_list_source_def = list(set_data_columns - set_source_def_attribute_name_list)
                self._unmatched_columns_list_data = list(set_source_def_attribute_name_list - set_data_columns)

        except Exception:
            logger.error("Error in Check Column Function of File Validation Class!!!", exc_info=True)

    def check_data_type(self):
        try:

            data_column_converter = {}
            for name in self._source_def_attribute_name_list:
                data_column_converter[name] = str

            if self._file_name.split(".")[-1] == "xlsb":
                self._excel_data = pd.read_excel(
                    self._file_name,
                    engine="pyxlsb",
                    skiprows=int(self._column_start_row) - 1,
                    usecols=self._source_def_attribute_name_list,
                    converters=data_column_converter
                )[self._source_def_attribute_name_list]

            elif self._file_name.split(".")[-1] == "xlsx" or self._file_name.split(".")[-1] == "xls":
                self._excel_data = pd.read_excel(
                    self._file_name,
                    skiprows=int(self._column_start_row) - 1,
                    usecols=self._source_def_attribute_name_list,
                    converters=data_column_converter
                )

            # work_book = xl.open_workbook(self._file_name)
            # sheet_1 = work_book.sheet_by_index(0)
            row_count = len(self._excel_data)
            col_count = len(self._source_def_attribute_name_list)
            for col in range(0, col_count):
                data_type = self._source_def_dict[col]['attribute_data_type']
                for row in range(self._column_start_row-1, row_count):
                    data = self._excel_data.iloc[row][col]
                    data_str = str(data)
                    if data_type == "decimal":
                        decimal_check_output = self.check_decimal_data_type(data_str = data_str)
                        if decimal_check_output == False:
                            self._incorrect_data_type_list_data.append(
                                {
                                    "column_position": (row + 1, col + 1),
                                    "data_str": data_str
                                }
                            )

                            break
                        else:
                            continue
                    elif data_type == "integer":
                        integer_check_output = self.check_integer_data_type(data_str = data_str)
                        if integer_check_output == False:
                            self._incorrect_data_type_list_data.append(
                                {
                                    "column_position": (row + 1, col + 1),
                                    "data_str": data_str
                                }
                            )
                            break
                        else:
                            continue

            print("incorrect_data_type_list")
            print(self._incorrect_data_type_list_data)

        except Exception:
            logger.error("Error in Check Data Type Integer Function of File Validation Class!!!", exc_info=True)

    def check_integer_data_type(self, data_str):
        try:
            if re.match(r'^-?\d+$', data_str) is not None:
                return True
            else:
                return False
        except Exception:
            logger.error("Error in Check Data Type Integer Function of File Validation Class!!!", exc_info=True)
            return False

    def check_decimal_data_type(self, data_str):
        try:
            # r'^-?\d+(?:\.\d+)$'
            if re.match(r'^-?[0-9]+\.?\d+?', data_str) is not None or re.match(r'^-?\d+$', data_str) is not None:
                return True
            else:
                return False
        except Exception:
            logger.error("Error in Check Data Type Decimal Function of File Validation Class!!!", exc_info=True)
            return False

    def get_file_exists(self):
        return self._file_exists

    def get_keyword_check(self):
        return self._keyword_check

    def get_check_column_count(self):
        return self._column_count

    def get_check_column_position(self):
        return self._position_check

    def get_mismatch_data_list(self):
        return self._mismatch_data_list

    def get_unmatched_column_list_source_def(self):
        return self._unmatched_columns_list_source_def

    def get_unmatched_column_list_data(self):
        return self._unmatched_columns_list_data

    def get_incorrect_data_type_list_data(self):
        return self._incorrect_data_type_list_data

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