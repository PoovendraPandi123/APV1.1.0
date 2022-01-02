import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import os
import api_properties as api
import data_request as dr
import etl_functions as ef
import json

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
                    etl_functions = ef.JobExecutions(jobs_list = jobs_list)
                    action_code_list = etl_functions.get_action_code_list()
                    processing_layers_jobs_list.append(
                        {
                            "m_processing_layer_id" : file["m_processing_layer_id"],
                            "m_processing_sub_layer_id" : file["m_processing_sub_layer_id"],
                            "processing_layer_id" : file["processing_layer_id"],
                            "action_code_list" : action_code_list
                        }
                    )
                # print(processing_layers_jobs_list)

            else:
                logging.info("Jobs Properties not found!!!")
        else:
            print("No File Found in Batch!!!")

    else:
        logging.info("Batch File Properties not found!!!")