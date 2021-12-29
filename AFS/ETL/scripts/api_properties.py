import logging
import os
import json

class APIProperties:

    _property_file = ''
    _api_properties = ''

    def __init__(self, property_folder, property_file):
        if os.path.exists(property_folder):
            self._property_file = property_file
            self.load_properties()
        else:
            print("API Properties Folder Not Found!!!")

    def load_properties(self):
        try:
            with open(self._property_file, "r") as f:
                self._api_properties = json.load(f)
        except Exception:
            logging.error("Error in Load API Properties Json!!!", exc_info=True)

    def get_api_properties(self):
        return self._api_properties