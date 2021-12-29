import send_request as sr

request = sr.SendRequest()

class GetResponse:
    _url = ''
    _header = ''
    _data = ''
    _response_data = ''

    def __init__(self, properties):
        self._url = properties["url"]
        self._header = properties["header"]
        self._data = properties["data"]
        self.response()

    def response(self):
        self._response_data = request.get_response(post_url = self._url, headers = self._header, data = self._data)

    def get_response_data(self):
        return self._response_data