import pandas as pd
from pyspark.sql import SparkSession

class ReadFile:

    _source_config = ''
    _source_file_path = ''
    _source_columns = ''
    _source_definitions_list = ''
    _source_file_type = ''
    _source_data = ''
    _source_data_spark_df = ''
    _spark = ''

    def __init__(self, spark, source_config, file_path, source_columns, source_definitions_list):
        self._spark = spark
        self._source_config = source_config
        self._source_file_path = file_path
        self._source_columns = source_columns
        self._source_definitions_list = source_definitions_list
        # print(self._source_config)
        # print(self._source_file_path)
        # print(self._source_columns)
        # print(self._source_definitions_list)
        for k,v in self._source_config.items():
            if k == "file_type":
                self._source_file_type = v

        if self._source_file_type == "excel":
            if self._source_file_path.lower().split(".")[-1] in ["xls", "xlsx"]:
                self.read_excel()

    def read_excel(self):
        self._source_data = pd.read_excel(self._source_file_path)
        self._source_data_spark_df = self._spark.createDataFrame(self._source_data.astype(str))

    def get_source_data(self):
        return self._source_data

    def get_source_data_spark_df(self):
        return self._source_data_spark_df


