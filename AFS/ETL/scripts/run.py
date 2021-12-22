import data_request as dr
import read_file as rf
import validate_file as vf
import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import json

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

def execute_etl(batch_file_properties, source_properties):
    try:
        # Batch Files
        batch_files = dr.BatchFiles(batch_file_properties)
        batch_files_list = batch_files.get_batch_files()
        # print(batch_files_list)

        # Loop through the Batch file list
        for batch_file in batch_files_list:
            for k, v in batch_file.items():
                if k == "m_source_id":
                    source_id = v
                if k == "file_path":
                    file_path = v
            source_properties["source_url"] = source_properties["source_url"].replace("{id}", str(source_id))
            # Sources
            sources = dr.Sources(source_properties)
            source_data = sources.get_sources()
            source_code = source_data.get('source_code', '')
            source_config = source_data.get('source_config', '')
            source_name = source_data.get('source_name', '')

            # Source Definitions
            source_definitions_list = source_data.get('m_source_definitions', '')
            if source_definitions_list and source_code and source_config and source_name:
                source_definitions_list_json = json.dumps(source_definitions_list)
                data_frame = sqlContext.read.json(sc.parallelize([source_definitions_list_json]))
                data_frame.createOrReplaceTempView("m_source_definitions")
                m_source_definition = sqlContext.sql("select * from m_source_definitions where is_active = 1 order by attribute_position asc")
                m_source_definition_map = m_source_definition.rdd.\
                    map(
                    lambda x : {
                        "attribute_name" : x.attribute_name,
                        "attribute_data_type" : x.attribute_data_type,
                        "is_validate" : x.is_validate,
                        "attribute_min_length" : x.attribute_min_length,
                        "attribute_max_length" : x.attribute_max_length
                    }
                )
                m_source_definition_list = m_source_definition_map.collect()
                source_columns = m_source_definition.rdd.map(
                    lambda x : x.attribute_name
                ).collect()
                read_file = rf.ReadFile(spark, source_config = source_config, file_path = file_path, source_columns = source_columns, source_definitions_list = m_source_definition_list, source_name = source_name)
                data_spark_df = read_file.get_source_data_spark_df()
                # print(data.show())
                # print(type(data))
                # print(data.printSchema())
                validate_attribute_row = m_source_definition_map.filter(lambda x : x["is_validate"] == 1)
                validate_attribute_row_list = validate_attribute_row.collect()
                validate_spark_df = vf.ValidateFile(spark_df = data_spark_df, validate_row = validate_attribute_row_list, df_columns = source_columns)
                validated_data = validate_spark_df.get_validated_df()
                # print("**********Validated Data************")
                # print(validated_data.show())
            return ''
    except Exception:
        logging.error("Error in Executing ETL!!!", exc_info=True)

if __name__ == "__main__":
    # Batch Files
    batch_file_properties = {
        "batch_files_url": "http://localhost:50010/api/v1/alcs/generic/file_uploads/?status=batch",
        "batch_files_header": {"Content-Type": "application/json"},
        "batch_files_data": ""
    }

    # Sources
    source_properties = {
        "source_url": "http://localhost:50003/api/v1/sources/source/{id}/",
        "source_header": {"Content-Type": "application/json"},
        "source_data": ""
    }
    execute_etl(batch_file_properties, source_properties)


