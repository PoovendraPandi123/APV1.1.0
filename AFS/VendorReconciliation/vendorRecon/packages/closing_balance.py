import pandas as pd
import logging
logger = logging.getLogger("vendor_reconciliation")

class VendorClosingBalance:

    _file_path = ''
    _column_start_row = ''
    _columns_list = []
    _source_extension = ''
    _source_type = ''
    _data = ''
    _vendor_closing_balance = 0
    _vendor_closing_balance_dr_cr = ''

    def __init__(self, file_path, column_start_row, columns_list, source_extension, source_type):
        self._file_path = file_path
        self._column_start_row = column_start_row
        self._columns_list = columns_list
        self._source_extension = source_extension
        self._source_type = source_type
        self.read_source_data()
        if self._source_type == "Tally":
            self.tally_closing_balance()

    def read_source_data(self):
        try:
            self._data = ''
            if self._source_extension in ["csv"]:
               self._data = pd.read_csv(self._file_path, skiprows = int(self._column_start_row) - 1, usecols = self._columns_list)[self._columns_list]
            elif self._source_extension in ["xls", "xlsx"]:
                self._data = pd.read_excel(self._file_path, skiprows = int(self._column_start_row) - 1, usecols = self._columns_list)[self._columns_list]
            elif self._source_extension in ["xlsb"]:
                self._data = pd.read_excel(self._file_path, skiprows = int(self._column_start_row) - 1, usecols = self._columns_list)[self._columns_list]
        except Exception:
            logger.error("Error in Read source Data Function of Vendor Closing Balance Class!!!", exc_info=True)

    def tally_closing_balance(self):
        try:
            if len(self._data) > 0:
                data_required = self._data[self._data["Unnamed: 2"] == 'Closing Balance']
                if len(data_required) == 1:
                    data_required_list = []
                    for index, row in data_required.iterrows():
                        data_list = [row[column] for column in data_required.columns]
                        data_required_list.append(data_list)

                        self._vendor_closing_balance = data_required_list[0][-1]
                        self._vendor_closing_balance_dr_cr = data_required_list[0][1]
        except Exception:
            logger.error("Error in Tally Closing Balance Function of Vendor Closing Balance Class!!!", exc_info=True)

    def get_vendor_closing_balance(self):
        return self._vendor_closing_balance

    def get_vendor_closing_balance_dr_cr(self):
        return self._vendor_closing_balance_dr_cr


