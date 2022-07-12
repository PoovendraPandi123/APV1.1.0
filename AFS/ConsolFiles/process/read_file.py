import logging
import pandas as pd
from .models import SourceDefinitions
from .packages import read_file as rf

logger = logging.getLogger("consolidation_files")

class ReadFile():

    _m_sources_id = 0
    _m_sources_name = ''
    _file_path = ''
    _source_def_attribute_list = []
    _source_def_data_type_list = []
    _source_def_unique_list = []
    _column_start_row = 0
    _read_file_output = pd.DataFrame()

    def __init__(self, m_sources_id, file_path, column_start_row, m_sources_name):
        self._m_sources_id = m_sources_id
        self._file_path = file_path
        self._column_start_row = column_start_row
        self._m_sources_name = m_sources_name
        self.update_source_details()
        self.read_file()

    def update_source_details(self):
        try:
            source_definitions = SourceDefinitions.objects.filter(sources_id = self._m_sources_id, is_active=1).order_by('attribute_position')

            self._source_def_attribute_list = []
            self._source_def_data_type_list = []
            self._source_def_unique_list = []

            for source_def in source_definitions:
                self._source_def_attribute_list.append(source_def.attribute_name)
                self._source_def_data_type_list.append(source_def.attribute_data_type)
                self._source_def_unique_list.append(source_def.is_unique)
        except Exception:
            logger.error("Error in Update Source Details Function in Read File Class!!!", exc_info=True)

    def read_file(self):
        try:
            read_file_output = rf.get_data_from_file(
                file_path = self._file_path,
                sheet_name = '',
                source_extension = self._file_path.split(".")[-1],
                attribute_list = self._source_def_attribute_list,
                column_start_row = self._column_start_row,
                password_protected = '',
                source_password = '',
                attribute_data_types_list = self._source_def_data_type_list,
                unique_list = self._source_def_unique_list,
                date_key_word = self._m_sources_name
            )

            if read_file_output["Status"] == "Success":
                self._read_file_output = read_file_output["data"]["data"]

        except Exception:
            logger.error("Error in Read File Function of Read File Class!!!", exc_info=True)

    def get_read_file_output(self):
        return self._read_file_output