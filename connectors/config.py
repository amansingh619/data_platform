import yaml

class ConfigLoader:
    """Class to read yaml file & return required values"""

    def __init__(self, path: str = "config/config.yaml"):
        with open(path, "r") as f:
            self.config = yaml.safe_load(f)

    def get(self, key: str, default=None):
        return self.config.get(key, default)