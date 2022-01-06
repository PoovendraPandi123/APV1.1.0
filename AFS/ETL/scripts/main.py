import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import os
import api_properties as api
import data_request as dr
import etl_functions as ef
from process import *
import json
import re

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

if __name__ == "__main__":
    config_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
    date_config_folder = config_folder
    date_config_file = os.path.join(date_config_folder, "dates.json")
    # API Calls
    api_properties_file = os.path.join(config_folder, "api_calls.json")

    api_properties = api.APIProperties(property_folder=config_folder, property_file=api_properties_file)
    api_properties_data = api_properties.get_api_properties()

    # Get the list of files in the BATCH
    batch_file_properties = api_properties_data.get("batch_file_properties", "")
    if batch_file_properties:
        batch_files = dr.GetResponse(batch_file_properties)
        batch_files_list = batch_files.get_response_data()
        # print(batch_files_list)
        if len(batch_files_list) > 0:
            batch_file_list_json = json.dumps(batch_files_list)
            batch_file_df = sqlContext.read.json(sc.parallelize([batch_file_list_json]))
            batch_file_df.createOrReplaceTempView("file_uploads")
            file_uploads = sqlContext.sql("select m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id from file_uploads group by m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id")
            file_uploads_map = file_uploads.rdd.\
                map(
                lambda x : {
                    "m_processing_layer_id" : x.m_processing_layer_id,
                    "m_processing_sub_layer_id" : x.m_processing_sub_layer_id,
                    "processing_layer_id" : x.processing_layer_id
                }
            )
            file_uploads_distinct_list = file_uploads_map.collect()
            # print(file_uploads_distinct_list)
            # List of Jobs for Processing Layers
            jobs_properties = api_properties_data.get("jobs_properties", "")
            if jobs_properties:
                processing_layers_jobs_list = []
                for file in file_uploads_distinct_list:
                    jobs_properties["url"] = jobs_properties["url"].replace("{m_processing_layer_id}", str(file["m_processing_layer_id"])).replace("{m_processing_sub_layer_id}", str(file["m_processing_sub_layer_id"])).replace("{processing_layer_id}", str(file["processing_layer_id"]))
                    jobs = dr.GetResponse(jobs_properties)
                    jobs_list = jobs.get_response_data()
                    job_executions = ef.JobExecutions(jobs_list = jobs_list)
                    action_code_list = job_executions.get_action_code_list()
                    processing_layers_jobs_list.append(
                        {
                            "m_processing_layer_id" : file["m_processing_layer_id"],
                            "m_processing_sub_layer_id" : file["m_processing_sub_layer_id"],
                            "processing_layer_id" : file["processing_layer_id"],
                            "action_code_list" : action_code_list
                        }
                    )
                if len(processing_layers_jobs_list) > 0:
                    for processing_layer_jobs in processing_layers_jobs_list:
                        file_uploads_sources_query = "select * from file_uploads where m_processing_layer_id = {m_processing_layer_id} and m_processing_sub_layer_id = {m_processing_sub_layer_id} and processing_layer_id = {processing_layer_id}".replace("{m_processing_layer_id}", str(processing_layer_jobs["m_processing_layer_id"])).replace("{m_processing_sub_layer_id}", str(processing_layer_jobs["m_processing_sub_layer_id"])).replace("{processing_layer_id}", str(processing_layer_jobs["processing_layer_id"]))
                        file_uploads_sources = sqlContext.sql(file_uploads_sources_query)
                        file_uploads_sources_map = file_uploads_sources.rdd.\
                            map(
                            lambda x : {
                                "file_id" : x.id,
                                "m_processing_layer_id" : x.m_processing_layer_id,
                                "m_processing_sub_layer_id" : x.m_processing_sub_layer_id,
                                "processing_layer_id" : x.processing_layer_id,
                                "file_path" : x.file_path,
                                "m_source_id" : x.m_source_id
                            }
                        )
                        file_uploads_sources_list = file_uploads_sources_map.collect()
                        # print(file_uploads_sources_list)
                        if len(file_uploads_sources_list) > 0:
                            source_1_file_path = ''
                            source_1_file_id = ''
                            source_1_source_id = ''
                            source_2_file_path = ''
                            source_2_file_id = ''
                            source_2_source_id = ''
                            for file_uploads_source in file_uploads_sources_list:
                                if re.search(r'bank', file_uploads_source["file_path"].split("/")[-3].lower()) and re.search(r'alcs', file_uploads_source["file_path"].split("/")[-3].lower()):
                                    source_2_file_path = file_uploads_source["file_path"]
                                    source_2_file_id = file_uploads_source["file_id"]
                                    source_2_source_id = file_uploads_source["m_source_id"]
                                elif re.search(r'alcs', file_uploads_source["file_path"].split("/")[-3].lower()):
                                    source_1_file_path = file_uploads_source["file_path"]
                                    source_1_file_id = file_uploads_source["file_id"]
                                    source_1_source_id = file_uploads_source["m_source_id"]

                            job_execution_id = 0
                            print("Creating Execution Id for Sources!!!")
                            execution_id_properties = api_properties_data.get("execution_id_properties", "")
                            if execution_id_properties:
                                execution_id = ef.JobExecutionId(
                                    m_processing_layer_id=file_uploads_sources_list[0]["m_processing_layer_id"],
                                    m_processing_sub_layer_id=file_uploads_sources_list[0]["m_processing_sub_layer_id"],
                                    processing_layer_id=file_uploads_sources_list[0]["processing_layer_id"],
                                    source_1_file_id=source_1_file_id,
                                    source_2_file_id=source_2_file_id,
                                    execution_id_properties=execution_id_properties
                                )
                                job_execution_id = execution_id.get_job_execution_id()
                                if int(job_execution_id) != 0:
                                    print("Starting ETL Process for Sources!!!")
                                    print("Reading Data!!!")
                                    source_properties = api_properties_data.get("source_properties", "")
                                    aggregator_details_properties = api_properties_data.get("aggregator_details_properties", "")
                                    field_extraction_properties = api_properties_data.get("field_extraction_properties", "")
                                    transformation_operators_properties = api_properties_data.get("transformation_operators_properties", "")
                                    source_definition_properties = api_properties_data.get("source_definition_properties", "")

                                    if source_properties and aggregator_details_properties and field_extraction_properties and transformation_operators_properties\
                                            and source_definition_properties:

                                        transformation_operators = dr.GetResponse(transformation_operators_properties)
                                        transformation_operators_list = transformation_operators.get_response_data()

                                        if len(str(source_1_source_id)) > 0 and len(str(source_2_source_id)) > 0:
                                            source_1_url_split = source_properties["url"].split("/")
                                            source_1_url_split[-2] = str(source_1_source_id)
                                            source_properties["url"] = "/".join(source_1_url_split)

                                            source_1_agg_details_url_split = aggregator_details_properties["url"].split("=")
                                            source_1_agg_details_url_split[-1] = str(source_1_source_id)
                                            aggregator_details_properties["url"] = "=".join(source_1_agg_details_url_split)
                                            source_1_agg_details_properties = aggregator_details_properties

                                            read_source_1_data = ef.ReadData(
                                                source_properties=source_properties,
                                                source_file_path=source_1_file_path,
                                                sqlContext=sqlContext,
                                                sparkContext=sc,
                                                spark=spark
                                            )
                                            source_1_spark_df = read_source_1_data.get_spark_read_df()
                                            source_1_columns = read_source_1_data.get_source_columns()
                                            source_1_validate_row_list = read_source_1_data.get_validate_attribute_row_list()
                                            source_1_date_transform_row_list = read_source_1_data.get_date_transform_attribute_row_list()
                                            source_1_name = read_source_1_data.get_source_name()

                                            source_2_url_split = source_properties["url"].split("/")
                                            source_2_url_split[-2] = str(source_2_source_id)
                                            source_properties["url"] = "/".join(source_2_url_split)

                                            source_2_agg_details_url_split = aggregator_details_properties["url"].split("=")
                                            source_2_agg_details_url_split[-1] = str(source_2_source_id)
                                            aggregator_details_properties["url"] = "=".join(source_2_agg_details_url_split)
                                            source_2_agg_details_properties = aggregator_details_properties

                                            read_source_2_data = ef.ReadData(
                                                source_properties=source_properties,
                                                source_file_path=source_2_file_path,
                                                sqlContext=sqlContext,
                                                sparkContext=sc,
                                                spark=spark
                                            )
                                            source_2_spark_df = read_source_2_data.get_spark_read_df()
                                            source_2_columns = read_source_2_data.get_source_columns()
                                            source_2_validate_row_list = read_source_2_data.get_validate_attribute_row_list()
                                            source_2_date_transform_row_list = read_source_2_data.get_date_transform_attribute_row_list()
                                            source_2_name = read_source_2_data.get_source_name()

                                            if len(source_1_spark_df.toPandas()) > 0 and len(source_2_spark_df.toPandas()) > 0:
                                                get_process_sources(
                                                    spark = spark,
                                                    sqlContext = sqlContext,
                                                    alcs_spark_df = source_1_spark_df,
                                                    bank_spark_df = source_2_spark_df,
                                                    action_code_list = processing_layer_jobs["action_code_list"],
                                                    source_1_columns = source_1_columns,
                                                    source_2_columns = source_2_columns,
                                                    validate_attribute_1_row_list = source_1_validate_row_list,
                                                    validate_attribute_2_row_list = source_2_validate_row_list,
                                                    date_transform_attribute_1_row_list = source_1_date_transform_row_list,
                                                    date_transform_attribute_2_row_list = source_2_date_transform_row_list,
                                                    source_1_name = source_1_name,
                                                    source_2_name = source_2_name,
                                                    date_config_folder = date_config_folder,
                                                    date_config_file = date_config_file,
                                                    aggregator_details_1_properties = source_1_agg_details_properties,
                                                    aggregator_details_2_properties = source_2_agg_details_properties,
                                                    field_extraction_properties = field_extraction_properties,
                                                    transformation_operators_list = transformation_operators_list,
                                                    source_definition_properties = source_definition_properties
                                                )
                                            elif len(source_1_spark_df) > 0 and len(source_2_spark_df) == 0:
                                                get_process_alcs(alcs_spark_df = source_1_spark_df, action_code_list = processing_layer_jobs["action_code_list"])
                                            elif len(source_2_spark_df) > 0 and len(source_1_spark_df) == 0:
                                                get_process_bank(bank_spark_df = source_2_spark_df, action_code_list = processing_layer_jobs["action_code_list"])
                                        else:
                                            pass
                                    else:
                                        print("Source Properties or Aggregator Details Properties or Field Extraction Properties or Transformation Operator Properties or Source Definition Properties not found!!!")
                                else:
                                    print("Job Execution Id is not created!!!")
                            else:
                                print("Execution Id Properties Not Found!!!")
                        else:
                            logging.info("Sources List Not Found!!!")
                else:
                    logging.info("Processing Layer Jobs List Not Found!!!")
            else:
                logging.info("Jobs Properties not found!!!")
        else:
            print("No File Found in Batch!!!")
    else:
        logging.info("Batch File Properties not found!!!")