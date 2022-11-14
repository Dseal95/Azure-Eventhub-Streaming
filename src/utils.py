"""Utils module comprises of generic functions."""
import os

import yaml


class Config:
    """A class to read / parse the .yaml config files stored in this root dir."""

    def __init__(self, file: str):
        """
        Assign the path of the .yaml file and read it. Stores a dict of the parameters in the class instance.

        :param file: (String) The filename, excluding the full path, of the config file to load.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(here, file)

        self._params = Config.load_yaml(path)

    def to_dict(self):
        """Return a dict of the parameters."""
        return self._params

    @staticmethod
    def load_yaml(filepath: str):
        """
        Read the yaml file.

        :param filepath: The full filepath (dealt with by the contructor) of the config file to be loaded.
        """
        try:
            with open(filepath) as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            raise FileNotFoundError("Failed to load config.")
