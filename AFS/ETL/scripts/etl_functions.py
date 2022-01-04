import logging
from datetime import datetime
import data_request as dr
import read_file as rf
import validate_file as vf
import transform_file as tf
import json

class JobExecutions:

    _jobs_list = ''
    _sequence_list = list()
    _actions_list = list()
    _execution_sequence_list = list()
    _action_sequence_list = list()
    _action_code_list = list()

    def __init__(self, jobs_list):
        self._jobs_list = jobs_list
        self.sequence_list()
        self.action_sequence_list()
        self.action_code_list()

    def sequence_list(self):
        try:
            for job in self._jobs_list:
                for k,v in job.items():
                    if k == "execution_sequence":
                        execution_sequence = v
                        for sequence in execution_sequence:
                            self._execution_sequence_list.append(sequence)
                            for k1,v1 in sequence.items():
                                if k1 == "sequence":
                                    self._sequence_list.append(v1)
                    if k == "actions":
                        actions = v
                        for action in actions:
                            self._actions_list.append(action)
            self._sequence_list.sort()

        except Exception:
            logging.error("Error in Getting Sequence List in Job Execution Class!!!", exc_info=True)

    def action_sequence_list(self):
        try:
            for sequence in self._sequence_list:
                for execution_sequence in self._execution_sequence_list:
                    if execution_sequence["sequence"] == sequence:
                        self._action_sequence_list.append(execution_sequence["actions"])
        except Exception:
            logging.error("Error in Getting Action Sequence List in Job Execution Class!!!", exc_info=True)

    def action_code_list(self):
        try:
            for action_sequence in self._action_sequence_list:
                for action in self._actions_list:
                    if action["actions_id"] == action_sequence:
                        self._action_code_list.append(action["action_code"])
        except Exception:
            logging.error("Error in Getting Action Code List in Job Execution Class!!!", exc_info=True)

    def get_action_code_list(self):
        return self._action_code_list

class JobExecutionId:

    _m_processing_layer_id = ''
    _m_processing_sub_layer_id = ''
    _processing_layer_id = ''
    _source_1_file_id = ''
    _source_2_file_id = ''
    _job_execution_id = 0
    _execution_id_properties = ''

    def __init__(self, m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id, source_1_file_id, source_2_file_id, execution_id_properties):
        self._m_processing_layer_id = m_processing_layer_id
        self._m_processing_sub_layer_id = m_processing_sub_layer_id
        self._processing_layer_id = processing_layer_id
        self._source_1_file_id = source_1_file_id
        self._source_2_file_id = source_2_file_id
        self._execution_id_properties = execution_id_properties
        self.create_job_execution_id()

    def create_job_execution_id(self):
        try:
            file_ids = []
            if len(str(self._source_1_file_id)) > 0 and len(str(self._source_2_file_id)) > 0:
                file_ids = [self._source_1_file_id, self._source_2_file_id]
            elif len(str(self._source_1_file_id)) > 0:
                file_ids = [self._source_1_file_id]
            elif len(str(self._source_2_file_id)) > 0:
                file_ids = [self._source_2_file_id]

            if len(file_ids) > 0:
                payload = json.dumps({
                    "m_processing_layer_id" : self._m_processing_layer_id,
                    "m_processing_sub_layer_id" : self._m_processing_sub_layer_id,
                    "processing_layer_id" : self._processing_layer_id,
                    "file_ids" : {"file_ids" : file_ids},
                    "execution_status" : "IN-PROGRESS",
                    "start_dt" : str(datetime.now()),
                    "end_dt" : str(datetime.now()),
                    "duration" : 0,
                    "executed_by" : 0,
                    "updated_by" : 0
                })
                self._execution_id_properties["data"] = payload
                post_data = dr.PostResponse(self._execution_id_properties)
                post_data_response = post_data.get_post_response_data()
                self._job_execution_id = post_data_response.get("job_execution_id", "0")
            else:
                print("There are no file ids to create job execution id!!!")
        except Exception:
            logging.error("Error in Creating Job Execution Id!!!", exc_info=True)

    def get_job_execution_id(self):
        return self._job_execution_id

class ReadData:

    _sqlContext = ''
    _sparkContext = ''
    _spark = ''
    _pandas_df = ''
    _spark_df = ''
    _m_source_definitions_map = ''
    _source_columns = ''
    _pandas_date_transformed_df = ''

    def __init__(self, source_properties, source_file_path, sqlContext, sparkContext, spark, date_config_folder, date_config_file):
        self._sqlContext = sqlContext
        self._sparkContext = sparkContext
        self._spark = spark
        self.read_data(source_properties, source_file_path, date_config_folder, date_config_file)

    def read_data(self, source_properties, source_file_path, date_config_folder, date_config_file):
        try:
            # print("Source URL")
            # print(source_properties["url"])
            sources = dr.GetResponse(source_properties)
            source_data = sources.get_response_data()
            # print("Source Response Data", source_data)
            source_code = source_data.get('source_code', '')
            source_config = source_data.get('source_config', '')
            source_name = source_data.get('source_name', '')
            source_definitions_list = source_data.get('m_source_definitions', '')
            if source_definitions_list and source_code and source_config and source_name:
                source_definitions_list_json = json.dumps(source_definitions_list)
                data_frame = self._sqlContext.read.json(self._sparkContext.parallelize([source_definitions_list_json]))
                data_frame.createOrReplaceTempView("m_source_definitions")
                m_source_definition = self._sqlContext.sql("select * from m_source_definitions where is_active = 1 order by attribute_position asc")
                self._m_source_definitions_map = m_source_definition.rdd. \
                    map(
                    lambda x: {
                        "attribute_name": x.attribute_name,
                        "attribute_data_type": x.attribute_data_type,
                        "is_validate": x.is_validate,
                        "attribute_min_length": x.attribute_min_length,
                        "attribute_max_length": x.attribute_max_length
                    }
                )
                m_source_definition_list = self._m_source_definitions_map.collect()
                self._source_columns = m_source_definition.rdd.map(lambda x: x.attribute_name).collect()
                read_file = rf.ReadFile(
                    spark = self._spark,
                    source_config = source_config,
                    file_path = source_file_path,
                    source_columns = self._source_columns,
                    source_definitions_list = m_source_definition_list,
                    source_name = source_name
                )
                self._spark_df = read_file.get_source_data_spark_df()
                if len(self._spark_df.toPandas()) > 0:
                    validate_attribute_row = self._m_source_definitions_map.filter(lambda x: x["is_validate"] == 1)
                    validate_attribute_row_list = validate_attribute_row.collect()
                    validate_file = vf.ValidateFile(spark_df = self._spark_df, validate_row = validate_attribute_row_list, df_columns = self._source_columns)
                    self._pandas_df = validate_file.get_validated_pandas_df()
                    date_transform_attribute_rows = self._m_source_definitions_map.filter(lambda x: x["attribute_data_type"] == "date")
                    date_transform_attribute_rows_list = date_transform_attribute_rows.collect()
                    transform_file = tf.TransformDate(
                        date_config_folder = date_config_folder,
                        df = self._pandas_df,
                        date_config_file = date_config_file,
                        attribute_row_list = date_transform_attribute_rows_list,
                        source_name = source_name
                    )
                    self._pandas_date_transformed_df = transform_file.get_pandas_df()

                else:
                    print("Size of Data Frame is equal to Zero!!!")
            else:
                print("Source Definitions List or Source Code or Source config or Source Name not found!!!")
        except Exception:
            logging.error("Error in Reading Data!!!", exc_info=True)

    def get_pandas_df(self):
        return self._pandas_df

    def get_pandas_date_transformed_df(self):
        return self._pandas_date_transformed_df
