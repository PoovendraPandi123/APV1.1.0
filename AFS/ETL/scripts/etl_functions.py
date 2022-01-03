import logging
from datetime import datetime
import data_request as dr

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
                payload = {
                    "m_processing_layer_id" : self._m_processing_layer_id,
                    "m_processing_sub_layer_id" : self._m_processing_sub_layer_id,
                    "file_ids" : {"file_ids" : file_ids},
                    "execution_status" : "IN-PROGRESS",
                    "start_dt" : str(datetime.now()),
                    "end_dt" : "",
                    "duration" : "",
                    "executed_by" : 0,
                    "updated_by" : 0
                }
                self._execution_id_properties["data"] = payload
                post_data = dr.PostResponse(self._execution_id_properties)
                post_data_response = post_data.get_post_response_data()
            else:
                print("There are no file ids to create job execution id!!!")
        except Exception:
            logging.error("Error in Creating Job Execution Id!!!", exc_info=True)

    def get_job_execution_id(self):
        return self._job_execution_id