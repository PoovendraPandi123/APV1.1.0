import logging
import os
import json

class ALCSFileProperties:

    _property_file = ''
    _alcs_properties = ''

    def __init__(self, property_folder, property_file):
        if os.path.exists(property_folder):
            self._property_file = property_file
            self.load_properties()
        else:
            print("ALCS File Properties Folder Not Found!!!")

    def load_properties(self):
        try:
            with open(self._property_file, "r") as f:
                self._alcs_properties = json.load(f)
        except Exception:
            logging.error("Error in Load ALCS Properties Json!!!", exc_info=True)

    def get_alcs_file_properties(self):
        return self._alcs_properties