import logging
import os
import json

class BankFileProperties:

    _property_file = ''
    _bank_properties = ''

    def __init__(self, property_folder, property_file):
        if os.path.exists(property_folder):
            self._property_file = property_file
            self.load_properties()
        else:
            print("BANK File Properties Folder Not Found!!!")

    def load_properties(self):
        try:
            with open(self._property_file, "r") as f:
                self._bank_properties = json.load(f)
        except Exception:
            logging.error("Error in Load BANK File Properties Json!!!", exc_info=True)

    def get_bank_file_properties(self):
        return self._bank_properties