import data_request as dr
import read_file as rf
import validate_file as vf
import transform_file as tf
import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import json
import os
import sys
import pyarrow.parquet as pq
import pyarrow as pa

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

def execute_etl(batch_file_properties, source_properties, aggregator_details_properties, aggregator_transformations_properties, field_extraction_properties, date_config_folder, date_config_file):
    try:
        # Batch Files
        batch_files = dr.GetResponse(batch_file_properties)
        batch_files_list = batch_files.get_response_data()
        # print(batch_files_list)

        # Loop through the Batch file list
        for batch_file in batch_files_list:
            for k, v in batch_file.items():
                if k == "m_source_id":
                    source_id = v
                if k == "file_path":
                    file_path = v
            source_properties["url"] = source_properties["url"].replace("{id}", str(source_id))
            # Sources
            sources = dr.GetResponse(source_properties)
            source_data = sources.get_response_data()
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
                read_file = rf.ReadFile(
                    spark = spark,
                    source_config = source_config,
                    file_path = file_path,
                    source_columns = source_columns,
                    source_definitions_list = m_source_definition_list,
                    source_name = source_name
                )
                data_spark_df = read_file.get_source_data_spark_df()
                # print(data.show())
                # print(type(data))
                # print(data.printSchema())
                # Validations
                validate_attribute_row = m_source_definition_map.filter(lambda x : x["is_validate"] == 1)
                validate_attribute_row_list = validate_attribute_row.collect()
                validate_spark_df = vf.ValidateFile(
                    spark_df = data_spark_df,
                    validate_row = validate_attribute_row_list,
                    df_columns = source_columns
                )
                validated_df = validate_spark_df.get_validated_df()
                # print("**********Validated Data************")
                # print(validated_df.show())
                # Transformations
                # 1) Date Transformations
                transform_attribute_rows = m_source_definition_map.filter(lambda x : x["attribute_data_type"] == "date")
                transform_attribute_rows_list = transform_attribute_rows.collect()
                date_transform_spark_df = tf.DateTransformations(
                    spark_df = validated_df,
                    attribute_row_list = transform_attribute_rows_list,
                    source_name = source_name,
                    df_columns = source_columns,
                    date_config_folder = date_config_folder,
                    date_config_file = date_config_file
                )
                date_transformed_df = date_transform_spark_df.get_date_transformed_df()
                # print(date_transformed_df.show())
                # 2) Removing Unnecessary characters in the Data frame
                char_attribute_rows = m_source_definition_map.filter(lambda x : x["attribute_data_type"] != "date")
                char_attribute_row_list = char_attribute_rows.collect()
                # print(char_attribute_row_list)
                field_transform = tf.FieldTransformation(
                    spark_session = spark,
                    spark_df = date_transformed_df,
                    attribute_list = char_attribute_row_list,
                    df_columns = source_columns
                )
                field_transformed_df = field_transform.get_field_transformed_df()
                # print(field_transformed_df.show())

                # Aggregators
                aggregator_details_properties["url"] = aggregator_details_properties["url"].replace("{m_source_id}", str(source_id))
                aggregator_details = dr.GetResponse(aggregator_details_properties)
                aggregator_details_data = aggregator_details.get_response_data()
                m_aggregator_id = aggregator_details_data[0]["m_aggregator"]

                aggregator_transformations_properties["url"] = aggregator_transformations_properties["url"].replace("{m_aggregator_id}", str(m_aggregator_id))
                aggregator_transformations = dr.GetResponse(aggregator_transformations_properties)
                aggregator_transformations_data = aggregator_transformations.get_response_data()
                m_aggregator_transforms = aggregator_transformations_data[0]["m_aggregator_transforms"]

                field_extraction_source = m_aggregator_transforms.get('field_extraction', '')
                lookup_extraction_source = m_aggregator_transforms.get('lookup_extraction', '')
                math_transformation_source = m_aggregator_transforms.get('math_transformation', '')

                if field_extraction_source:
                    field_extraction_properties["url"] = field_extraction_properties["url"].replace("{m_aggregator_id}", str(m_aggregator_id))
                    field_extraction = dr.GetResponse(field_extraction_properties)
                    field_extraction_data = field_extraction.get_response_data()
                    field_extraction_source_ids_list = []
                    for data in field_extraction_data:
                        field_extraction_source_ids_list.append(data["m_sources_id"])
                    if source_id in field_extraction_data:
                        print("Exists")
                    else:
                        # reference_columns = m_source_definition.rdd.map(
                        #     lambda x: x.attribute_reference_field
                        # ).collect()
                        # field_transformed_df_rdd = field_transformed_df.rdd.map(
                        #     lambda x : x
                        # )
                        pandas_df = field_transformed_df.toPandas()
                        # pandas_df.to_parquet("G:/AdventsProduct/V1.1.0/AFS/ETL/data/output/out.parquet")
                        output_path = "G:/AdventsProduct/V1.1.0/AFS/ETL/data/output/out_table.parquet"
                        table = pa.Table.from_pandas(pandas_df)
                        pq.write_to_dataset(table, root_path=output_path)


                if lookup_extraction_source:
                    pass
                if math_transformation_source:
                    pass

            return ''
    except Exception:
        logging.error("Error in Executing ETL!!!", exc_info=True)

if __name__ == "__main__":
    # Batch Files
    batch_file_properties = {
        "url": "http://localhost:50010/api/v1/alcs/generic/file_uploads/?status=batch",
        "header": {"Content-Type": "application/json"},
        "data": ""
    }

    # Sources
    source_properties = {
        "url": "http://localhost:50003/api/v1/sources/source/{id}/",
        "header": {"Content-Type": "application/json"},
        "data": ""
    }

    # Aggregator Details
    aggregator_details_properties = {
        "url": "http://localhost:50003/api/v1/sources/generic/aggregator_details/?m_source_id={m_source_id}",
        "header" : {"Content-Type": "application/json"},
        "data" : ""
    }

    # Aggregator Transformations
    aggregator_transformations_properties = {
        "url": "http://localhost:50003/api/v1/sources/generic/aggregator_transformations/?m_aggregator_id={m_aggregator_id}",
        "header" : {"Content-Type": "application/json"},
        "data" : ""
    }

    # Field Extraction
    field_extraction_properties = {
        "url": "http://localhost:50003/api/v1/sources/generic/field_extraction/?m_aggregator_id={m_aggregator_id}",
        "header" : {"Content-Type": "application/json"},
        "data" : ""
    }

    # Date
    date_config_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
    date_config_file = os.path.join(date_config_folder, "dates.json")

    execute_etl(batch_file_properties = batch_file_properties,
                source_properties = source_properties,
                aggregator_details_properties = aggregator_details_properties,
                aggregator_transformations_properties = aggregator_transformations_properties,
                field_extraction_properties = field_extraction_properties,
                date_config_folder = date_config_folder,
                date_config_file = date_config_file
                )


