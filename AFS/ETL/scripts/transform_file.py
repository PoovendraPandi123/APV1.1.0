from datetime import datetime
import logging
import re
import os
import json
from pyspark.sql.functions import regexp_replace, translate

class DateTransformations:

    _spark_df = ''
    _attribute_row_list = ''
    _date_transformed_df = ''
    _source_name = ''
    _date_config_folder = ''
    _date_config_file = ''
    _month_list = ''
    _month_values = ''
    _df_columns = ''

    def __init__(self, spark_df, attribute_row_list, source_name, df_columns, date_config_folder, date_config_file):
        try:
            self._spark_df = spark_df
            self._attribute_row_list = attribute_row_list
            self._source_name = source_name
            self._date_config_folder = date_config_folder
            self._df_columns = df_columns

            if os.path.exists(date_config_folder):
                self._date_config_file = date_config_file
                self.load_date_json()
            else:
                print("Date Config Folder Not Found!!!")

            for attribute_row in self._attribute_row_list:
                self.date_transform(attribute_row["attribute_name"], self._source_name, self._df_columns)

        except Exception:
            logging.error("Error in init function of Transformations Class!!!", exc_info=True)

    def load_date_json(self):
        try:
            with open(self._date_config_file, "r") as f:
                file_data = json.load(f)

            self._month_list = file_data["month_list"]
            self._month_values = file_data["month_values"]

        except Exception:
            logging.error("Error in Load Date Json!!!", exc_info=True)

    def date_transform(self, attribute_name, source_name, df_columns):

        global month_list, month_values

        month_list = self._month_list
        month_values = self._month_values

        def convert_date(x, validate_column_index, source_name):
            try:
                # print("date_string ", date_string)
                # return (x[validate_column_index])
                if len(str(x[validate_column_index])) > 1:
                    excel_date = int(x[validate_column_index])
                    dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
                    # tt = dt.timetuple()
                    return (x, (str(dt),))
                elif len(str(x[validate_column_index])) < 1:
                    return (x, (x[validate_column_index],))
            except Exception:
                date_string = x[validate_column_index]

                year_hiffen = ''
                month_hiffen = ''
                day_hiffen = ''
                time_and_second = " 00:00:00"

                if re.search("alcs", source_name.lower()):
                    year_hiffen = date_string.split("-")[2]
                    month_hiffen = date_string.split("-")[1]
                    day_hiffen = date_string.split("-")[0]

                if month_hiffen in month_list:
                    year = year_hiffen
                    day = day_hiffen
                    if len(year_hiffen) == 2:
                        year = "20" + year_hiffen
                    if len(day_hiffen) == 1:
                        day = "0" + day_hiffen
                    month_value = month_values[month_hiffen]
                    output_date = year + "-" + month_value + "-" + day + time_and_second
                    return (x, (output_date,))
                return (x, (date_string,))

        def update_transform(transform_list, validate_column_index):
            try:
                transform_list_pop_last_index_value = transform_list.pop()
                transform_list.pop(validate_column_index)
                transform_list.insert(validate_column_index, transform_list_pop_last_index_value)
                return tuple(transform_list)
            except Exception:
                return tuple(transform_list)

        try:
            validate_column_index = self._df_columns.index(attribute_name)
            if self._spark_df and attribute_name:
                transform_rdd = self._spark_df.rdd.map(
                    lambda x: convert_date(x, validate_column_index, source_name)
                )

                transform_rdd_added = transform_rdd.map(
                    lambda x : update_transform(list(x[0] + x[1]), validate_column_index)
                )
                self._date_transformed_df = transform_rdd_added.toDF(df_columns)
        except Exception:
            logging.error("Error in Date Transform Function!!!", exc_info=True)

    def get_date_transformed_df(self):
        return self._date_transformed_df

class FieldTransformation:

    _spark_df = ''
    _attribute_list = ''
    _spark_session = ''
    _df_columns = ''

    def __init__(self, spark_session, spark_df, attribute_list, df_columns):
        try:
            self._spark_df = spark_df
            self._attribute_list = attribute_list
            self._spark_session = spark_session
            self._df_columns = df_columns
            self.field_transform()
        except Exception:
            logging.error("Error in Init Function of Field Transformation Class!!!", exc_info=True)

    def field_transform(self):
        try:
            if self._spark_df and self._attribute_list and self._df_columns:
                for i in range(0, len(self._attribute_list)):
                    attribute = self._attribute_list[i].get("attribute_name", "")
                    if attribute:
                        attribute_column_index = self._df_columns.index(attribute)
                        single_quote_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], "'", "q@t"))
                        self._spark_df = single_quote_df
                        sp_character_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], "^\s+$", ""))
                        self._spark_df = sp_character_df
                        tab_separated_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], "/\\t/", ""))
                        self._spark_df = tab_separated_df
                        new_line_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], "/\\n/", ""))
                        self._spark_df = new_line_df
                        back_slash_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], "/\\/", "s@l"))
                        self._spark_df = back_slash_df
                        double_quote_df = self._spark_df.withColumn(attribute, regexp_replace(self._spark_df[attribute_column_index], '"', "q@@t"))
                        self._spark_df = double_quote_df
        except Exception:
            logging.error("Error in Field Transform Function of Field Transformation Class!!!", exc_info=True)

    def get_field_transformed_df(self):
        return self._spark_df
