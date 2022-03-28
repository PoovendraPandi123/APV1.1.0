import pandas as pd
import numpy as np
import logging

class ReadFile:

    _source_name = ''
    _source_config = ''
    _source_file_path = ''
    _source_columns = ''
    _source_definitions_list = ''
    _source_file_type = ''
    _source_start_row = ''
    _source_data = ''
    _source_data_spark_df = ''
    _source_pandas_df = ''
    _spark = ''

    def __init__(self, spark, source_config, file_path, source_columns, source_definitions_list, source_name):
        try:
            self._spark = spark
            self._source_config = source_config
            self._source_file_path = file_path
            self._source_columns = source_columns
            self._source_definitions_list = source_definitions_list
            self._source_name = source_name
            # print(self._source_config)
            # print(self._source_file_path)
            # print(self._source_columns)
            # print(self._source_definitions_list)
            for k,v in self._source_config.items():
                if k == "file_type":
                    self._source_file_type = v
                if k == "column_start_row":
                    self._source_start_row = v

            if self._source_file_type == "excel":
                if self._source_file_path.lower().split(".")[-1] in ["xls", "xlsx"]:
                    self.read_excel()
                elif self._source_file_path.lower().split(".")[-1] in ["xlsb"]:
                    self.read_excel_binary()
        except Exception:
            logging.error("Error in Init Function of Read File Class!!!", exc_info = True)

    def read_excel(self):
        try:
            data_column_converter = {}
            for name in self._source_columns:
                data_column_converter[name] = str

            self._source_data = pd.read_excel(self._source_file_path, usecols = self._source_columns, skiprows = int(self._source_start_row) - 1, converters=data_column_converter)[self._source_columns]
            if len(self._source_data) > 0:
                data_proper = self._source_data.replace(np.nan, '')
                self._source_pandas_df = data_proper
                self._source_data_spark_df = self._spark.createDataFrame(data_proper.astype(str))

        except Exception:
            logging.error("Error in Read Excel!!!", exc_info = True)

    def read_excel_binary(self):
        pass

    def get_source_data(self):
        return self._source_data

    def get_source_pandas_df(self):
        return self._source_pandas_df

    def get_source_data_spark_df(self):
        return self._source_data_spark_df


