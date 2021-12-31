import logging
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext, SparkSession
import os
import api_properties as api
import data_request as dr

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

        # List of Jobs for Processing Layers
        jobs_properties = api_properties_data.get("jobs_properties", "")
        if jobs_properties:
            for file in batch_files_list:
                jobs_properties["url"] = jobs_properties["url"].replace("{m_processing_layer_id}", str(file["m_processing_layer_id"])).replace("{m_processing_sub_layer_id}", str(file["m_processing_sub_layer_id"])).replace("{processing_layer_id}", str(file["processing_layer_id"]))
                jobs = dr.GetResponse(jobs_properties)
                jobs_list = jobs.get_response_data()
                print(jobs_list)
        else:
            logging.info("Jobs Properties not found!!!")
    else:
        logging.info("Batch File Properties not found!!!")