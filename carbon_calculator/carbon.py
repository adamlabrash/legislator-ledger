"carbon.py contains the main class Carbon, from which all transport modes will inherit"

import os
import json


class Carbon(object):
    """Main class to calculate CO2 emmissions for different modes of transportation."""

    def dict_from_json(self, relative_path):
        """relative_path must include a leading slash and the format of the file, as well as any possible subfolders:
        '/subfolder/onefile.json'"""

        json_folder = os.path.dirname(os.path.abspath(__file__))
        json_path = json_folder + relative_path

        with open(json_path) as json_data:
            json_as_dict = json.loads(json_data.read())

            json_data.close()

        return json_as_dict
