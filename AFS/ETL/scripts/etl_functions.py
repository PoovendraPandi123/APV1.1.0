import logging

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