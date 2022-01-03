import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import os
import api_properties as api
import data_request as dr
import etl_functions as ef
import json
import re

sc = SparkContext(master="local", appName="ETL")
sqlContext = SQLContext(sc)
spark = SparkSession.getActiveSession()

if __name__ == "__main__":
    config_folder = 'G:/AdventsProduct/V1.1.0/AFS/ETL/config'
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

                            if len(str(source_1_source_id)) > 0 and len(str(source_2_source_id)) > 0:
                                print("Start ETL Processing for both the sources!!!")
                                execution_id_properties = api_properties_data.get("execution_id_properties", "")
                                if execution_id_properties:
                                    execution_id = ef.JobExecutionId(
                                        m_processing_layer_id = file_uploads_sources_list[0]["m_processing_layer_id"]
                                    )
                                else:
                                    print("Execution Id Properties Not Found!!!")
                            elif len(str(source_1_source_id)) > 0:
                                print("Start ETL Processing for ALCS!!!")
                            elif len(str(source_2_source_id)) > 0:
                                print("Start ETL Processing for BANK!!!")

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