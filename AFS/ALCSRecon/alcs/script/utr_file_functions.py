import re

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("alcs_recon")

month_values = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec"
}


class ValidateUTRFile:

    _utr_file_default_columns = ['Slno', 'Client_ID', 'Emp_NO', 'Transaction_Month', 'Transaction_Year', 'Processing_Month', 'Processing_Year', 'Pay_Type', 'Invoice_No', 'Release_Date', 'NetPay_Amount', 'UTR_Number', 'Debit_Date']
    _utr_file_path = ''
    _utr_file_columns = []
    _utr_columns_length = 13
    _utr_proper_data = pd.DataFrame()

    def __init__(self, utr_file_path):
        self._utr_file_path = utr_file_path
        self.read_file()

    def read_file(self):
        try:
            data = pd.read_csv(self._utr_file_path)
            self._utr_file_columns = data.columns
        except Exception:
            logger.error("Error in Read File Function in Validate UTR File Class!!!")

    def check_utr_columns(self):
        try:
            if len(self._utr_file_columns) >= self._utr_columns_length:
                count = 0
                for i in range(0, len(self._utr_file_default_columns)):
                    if self._utr_file_default_columns[i] == self._utr_file_columns[i]:
                        count = count + 1

                if count == self._utr_columns_length:
                    return "Success"
                else:
                    return "ColumnMismatch"
            else:
                return "ColumnCount"

        except Exception:
            logger.error("Error in Check UTR Column Function in Validate UTR File Class!!!", exc_info=True)
            return "Error"

    def read_file_proper(self):
        try:
            data_column_converter = {}
            for name in self._utr_file_default_columns:
                data_column_converter[name] = str

            data = pd.read_csv(self._utr_file_path, usecols=self._utr_file_default_columns, converters=data_column_converter)[self._utr_file_default_columns]

            if data.empty:
                return "NoData"
            elif not data.empty:
                self._utr_proper_data = data.replace(np.nan, '')
                return "Success"

        except Exception:
            logger.error("Error in Read File Proper Function in Validate UTR File Class!!!", exc_info=True)
            return "Error"

    def update_utr_values(self, internal_records_list):
        try:
            internal_data = pd.DataFrame(internal_records_list)
            utr_data = self._utr_proper_data

            internal_data[['int_amount_1']] = internal_data[['int_amount_1']].apply(pd.to_numeric)
            utr_data[['NetPay_Amount']] = utr_data[['NetPay_Amount']].apply(pd.to_numeric)

            output_file = pd.merge(
                utr_data,
                internal_data,
                how='left',
                left_on=['Emp_NO', 'Client_ID', 'Invoice_No', 'NetPay_Amount'],
                right_on=['int_reference_text_7', 'int_reference_text_8', 'int_reference_text_10', 'int_amount_1']
            )
            output_file['UTR_Number'] = output_file['int_reference_text_14']
            output_file['Debit_Date'] = output_file['int_reference_date_time_2']
            output_file.drop(['int_reference_text_1', 'int_reference_date_time_1', 'int_amount_1', 'int_reference_text_4', 'int_reference_text_5', 'int_reference_text_6', 'int_reference_text_7', 'int_reference_text_8', 'int_reference_text_9', 'int_reference_text_10', 'int_reference_text_11', 'int_reference_text_14', 'int_reference_date_time_2'], axis=1, inplace=True)
            output_file['UTR_Number'] = output_file['UTR_Number'].apply(self.convert_string)
            # output_file["Debit_Date"] = output_file["Debit_Date"].apply(self.convert_proper_date_format)

            output_file_path = self._utr_file_path.replace("Input", "Output")
            # output_file_without_nan = output_file.replace(np.nan, '')
            output_file.to_csv(output_file_path, index=False, na_rep="")

            report_url = "http://localhost:50010/static/UTR/Output/" + output_file_path.split("/")[-1]

            return {"Status": "Success", "report_url": report_url}
        except Exception:
            logger.error("Error in Updating UTR Values in Validate UTR File Class!!!", exc_info=True)
            return {"Status": "Error"}

    def convert_string(self, input_value):
        try:
            if len(str(input_value)) > 0 and str(input_value) != 'nan':
                if re.search(r'^[0-9]+', str(input_value)):
                    return "'" + str(input_value)
                else:
                    return str(input_value)
            elif len(str(input_value)) > 0 and str(input_value) == 'nan':
                return np.nan
            elif len(str(input_value)) == 0:
                return np.nan
        except Exception:
            logger.error("Error in Convert String Function in Validate UTR File Class!!!", exc_info=True)

    def convert_proper_date_format(self, input_date_string):
        try:
            if len(str(input_date_string)) > 0:
                date_split = str(input_date_string).split(" ")[0]
                day = date_split.split("-")[-1]
                month = date_split.split("-")[-2]
                year = date_split.split("-")[0]
                return day + "-" + month_values[month] + "-" + year
            else:
                return input_date_string
        except Exception:
            logger.error("Error in Convert Proper Date Format Function in Validate UTR File Class!!!", exc_info=True)
            return ""

    def get_utr_proper_data(self):
        return self._utr_proper_data