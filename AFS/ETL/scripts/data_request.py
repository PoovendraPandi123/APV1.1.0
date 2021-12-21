import send_request as sr

request = sr.SendRequest()

class BatchFiles:

    _batch_files_url = ''
    _headers_batch_files = ''
    _data_batch_files = ''
    _batch_file_list = ''

    def __init__(self, batch_files_prop):
        self._batch_files_url = batch_files_prop["batch_files_url"]
        self._headers_batch_files = batch_files_prop["batch_files_header"]
        self._data_batch_files = batch_files_prop["batch_files_data"]
        self.batch_files()

    def batch_files(self):
        self._batch_file_list = request.get_response(post_url = self._batch_files_url, headers = self._headers_batch_files, data = self._data_batch_files)

    def get_batch_files(self):
        return self._batch_file_list

class Sources:

    _source_url = ''
    _header_source = ''
    _data_source = ''
    _source_data = ''

    def __init__(self, source_prop):
        self._source_url = source_prop["source_url"]
        self._header_source = source_prop["source_header"]
        self._data_source = source_prop["source_data"]
        self.sources()

    def sources(self):
        self._source_data = request.get_response(post_url = self._source_url, headers = self._header_source, data = self._data_source)

    def get_sources(self):
        return self._source_data