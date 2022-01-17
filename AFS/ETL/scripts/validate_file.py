import logging

class ValidateFile:

    _spark_df = ''
    _validate_row = ''
    _validate_attribute = ''
    _validate_attribute_data_type = ''
    _validate_attribute_min_length = ''
    _validate_attribute_max_length = ''
    _df_columns = ''
    _validated_df = ''
    _validated_pandas_df = ''

    def __init__(self, spark_df, validate_row, df_columns):
        try:
            self._spark_df = spark_df,
            self._validate_row = validate_row
            self._df_columns = df_columns
            self.validate()
        except Exception:
            logging.error("Error in Init Function of Validate File!!!", exc_info=True)

    def validate(self):
        try:
            self._validate_attribute = self._validate_row[0].get('attribute_name', '')
            self._validate_attribute_data_type = self._validate_row[0].get('attribute_data_type', '')
            self._validate_attribute_min_length = self._validate_row[0].get('attribute_min_length', '')
            self._validate_attribute_max_length = self._validate_row[0].get('attribute_max_length', '')

            # print("Attribute Name", self._validate_attribute)
            # print("Attribute Data Type", self._validate_attribute_data_type)
            # print("Attribute Min Length", self._validate_attribute_min_length)
            # print("Attribute Max Length", self._validate_attribute_max_length)
            # print("Data")
            print(self._spark_df[0].show())
            # spark_df = self._spark_df[0]

            validate_column_index = self._df_columns.index(self._validate_attribute)
            attribute_min_length = int(self._validate_attribute_min_length)
            attribute_max_length = int(self._validate_attribute_max_length)

            # print("Source Columns", self._df_columns)
            # print("Validate Column Index", validate_column_index)

            if len(str(validate_column_index)) > 0 and len(str(attribute_min_length)) > 0 and len(str(attribute_max_length)):
                validate_df_rdd = self._spark_df[0].rdd.map\
                    (
                        lambda x : "False" if ( ( len(str(x[validate_column_index])) < int(attribute_min_length) ) or
                                              ( len(str(x[validate_column_index])) > int(attribute_max_length) ) ) else x
                    ).filter\
                    (
                        lambda x : x != "False"
                    )
                # validate_df_rdd = spark_df.rdd
                # for i in validate_df_rdd.take(validate_df_rdd.count()):
                #     print(i)
                self._validated_df = validate_df_rdd.toDF(self._df_columns)
                self._validated_pandas_df = self._validated_df.toPandas()
                # print("validated_pandas_df")
                # print(self._validated_pandas_df.tail(10))
        except Exception:
            logging.error("Error in Validating Function!!!", exc_info=True)

    def get_validated_df(self):
        return self._validated_df

    def get_validated_pandas_df(self):
        return self._validated_pandas_df