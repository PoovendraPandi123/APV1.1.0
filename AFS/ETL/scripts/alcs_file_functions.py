import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime

class ALCSConsolidatedFileValidation:

    _file_path = ''
    _alcs_consolidated_data = ''
    _alcs_validate_output = False
    _alcs_comment = ''
    _alcs_error_position = 0
    _alcs_error_text = ''

    def __init__(self, file_path):
        self._file_path = file_path
        self.validate_file()

    def validate_file(self):
        try:
            if len(self._file_path) > 0:
                data = pd.read_excel(self._file_path)
                columns = data.columns

                data_column_converter = {}
                for name in columns:
                    data_column_converter[name] = str

                data = pd.read_excel(self._file_path, usecols=columns, converters=data_column_converter)[columns]
                data_proper = data.replace(np.nan, '')

                self._alcs_consolidated_data = data_proper

                validate_number = 0
                for i in range(0, len(self._alcs_consolidated_data)):
                    if self.check_pattern(test_string = self._alcs_consolidated_data['PM_Payment_Date'][i]):
                        validate_number = validate_number + 1
                    else:
                        validate_number = 0
                        self._alcs_error_text = self._alcs_consolidated_data['PM_Payment_Date'][i]
                        self._alcs_error_position = i + 1
                        self._alcs_comment = "Error in 'PM Payment Date' Column!!!"
                        break

                validated_char_num = 0
                for i in range(0, len(self._alcs_consolidated_data)):
                    if self.check_enum(test_string = self._alcs_consolidated_data['Bank Name'][i]):
                        validated_char_num = validated_char_num + 1
                    else:
                        validated_char_num = 0
                        self._alcs_error_text = self._alcs_consolidated_data['Bank Name'][i]
                        self._alcs_error_position = i + 1
                        self._alcs_comment = "Error in 'Bank Name' Column!!!"
                        break


                if validate_number == 0 or validated_char_num == 0:
                    self._alcs_validate_output = False

                elif ( validate_number == len(self._alcs_consolidated_data) ) and ( validated_char_num == len(self._alcs_consolidated_data) ):
                    self._alcs_validate_output = True

            else:
                print("Length of File Path is equals to Zero!!!")

        except Exception:
            logging.error("Error in Validate File Function of ALCSConsolidatedFileValidation Class!!!", exc_info=True)

    def check_pattern(self, test_string):
        try:
            # print(test_string)
            # matched = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+', str(test_string))
            matched = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}', str(test_string))
            is_match = bool(matched)
            return is_match
        except Exception as e:
            print(e)
            logging.error("Error in Check Pattern Function of ALCSConsolidatedFileValidation Class!!!", exc_info=True)

    def check_enum(self, test_string):
        try:
            # if test_string in ['AXIS', 'ICICI', 'HDFC', 'SBI', 'HDFC-NEFT', 'ICICI NEFT']:
            if test_string == "AXIS" or test_string == "ICICI" or test_string == "HDFC" or test_string == "SBI" or test_string == "HDFC-NEFT" or test_string == "ICICI NEFT":
                print("True")
                return True
            else:
                print("False")
                return False
        except Exception as e:
            print(e)
            logging.error("Error in Check Enum Function of ALCSConsolidatedFileValidation Class!!!", exc_info=True)

    def get_validate_output(self):
        return self._alcs_validate_output

    def get_consolidated_alcs_data(self):
        return self._alcs_consolidated_data

    def get_error_text(self):
        return self._alcs_error_text

    def get_error_position(self):
        return self._alcs_error_position

    def get_error_comment(self):
        return self._alcs_comment

class ALCSConsolidatedFileSplit:

    _data_frame = ''
    _bank_column = ''
    _alcs_bank_name = ''
    _alcs_file_path = ''
    _alcs_file_name = ''

    def __init__(self, data_frame, bank_column, alcs_bank_name, alcs_file_path, alcs_file_name):
        self._data_frame = data_frame
        self._bank_column = bank_column
        self._alcs_bank_name = alcs_bank_name
        self._alcs_file_path = alcs_file_path
        self._alcs_file_name = alcs_file_name

    def store_alcs_file(self):
        """

        :return:
        """
        try:
            filtered_df = self._data_frame[self._data_frame[self._bank_column] == self._alcs_bank_name]
            # print("Filtered DF")
            # print(filtered_df)

            if len(filtered_df) > 0:
                now = datetime.now().strftime("%d_%m_%Y")  # "%d_%m_%Y_%H_%M_%S"
                filtered_df.to_excel(self._alcs_file_path + "/" + self._alcs_file_name + "_" + now + ".xlsx", header=True,
                                     index=False)
                return self._alcs_file_path + "/" + self._alcs_file_name + "_" + now + ".xlsx"
            else:
                print("No data available in given file :", self._alcs_file_name)
                return False
        except Exception as e:
            print(e)
            logging.error("Error in storing the ALCS file in ALCSConsolidatedFileSplit Class!!!", exc_info=True)