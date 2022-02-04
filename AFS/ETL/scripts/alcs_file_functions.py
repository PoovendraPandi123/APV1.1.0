import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime

class ALCSConsolidatedFileValidation:

    _file_path = ''
    _alcs_consolidated_data = ''
    _alcs_validate_output = bool

    def __init__(self, file_path):
        self._file_path = file_path
        self.validate_file()

    def validate_file(self):
        try:
            if len(self._file_path) > 0:
                data = pd.read_excel(self._file_path)
                data_proper = data.replace(np.nan, '')
                self._alcs_consolidated_data = data_proper
                validate_number = 1
                for i in range(0, len(self._alcs_consolidated_data)):
                    if self.check_pattern(test_string = self._alcs_consolidated_data['PM_Payment_Date'][i]):
                        validate_number = validate_number + 1
                    else:
                        validate_number = 1
                if validate_number == 1:
                    self._alcs_validate_output = False
                elif validate_number == len(self._alcs_consolidated_data):
                    self._alcs_validate_output = True

            else:
                print("Length of File Path is equals to Zero!!!")

        except Exception:
            logging.error("Error in Validate File Function of ALCSConsolidatedFileValidation Class!!!", exc_info=True)

    def check_pattern(self, test_string):
        try:
            matched = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+', str(test_string))
            is_match = bool(matched)
            return is_match
        except Exception as e:
            print(e)
            logging.error("Error in Check Pattern Function of ALCSConsolidatedFileValidation Class!!!", exc_info=True)

    def get_validate_output(self):
        return self._alcs_validate_output

    def get_consolidated_alcs_data(self):
        return self._alcs_consolidated_data

class ALCSConsolidatedFileSplit:

    def __int__(self, data_frame):
        pass

    def store_alcs_file(self, data_frame, bank_column, alcs_bank_name, alcs_file_path, alcs_file_name):
        """

        :return:
        """
        try:
            filtered_df = data_frame[data_frame[bank_column] == alcs_bank_name]
            # print("Filtered DF")
            # print(filtered_df)

            if len(filtered_df) > 0:
                now = datetime.now().strftime("%d_%m_%Y")  # "%d_%m_%Y_%H_%M_%S"
                filtered_df.to_excel(alcs_file_path + "/" + alcs_file_name + "_" + now + ".xlsx", header=True,
                                     index=False)
            else:
                print("No data available in given file :", alcs_file_name)
        except Exception as e:
            print(e)
            logging.error("Error in storing the ALCS file in ALCSConsolidatedFileSplit Class!!!", exc_info=True)