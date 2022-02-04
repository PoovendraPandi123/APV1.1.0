import zipfile
import logging
import os

class BankFileExtract:

    _zip_file_path = ''

    def __init__(self, zip_file_path):
        self._zip_file_path = zip_file_path

    def get_extract_bank_files(self):
        try:
            extract_path = self._zip_file_path.split("input/")[0] + "extract"

            if not os.path.exists(extract_path):
                os.mkdir(extract_path)

                with zipfile.ZipFile(self._zip_file_path, 'r') as zip_file:
                    zip_file.extractall(extract_path)
                return {"Status": "Success", "path": extract_path}
            else:
                return {"Status": "Exists"}
        except Exception as e:
            print(e)
            logging.error("Error in Bank File Extract Function!!!", exc_info=True)
            return {"Status": "Error"}