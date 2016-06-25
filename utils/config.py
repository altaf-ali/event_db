import os
import yaml

import utils.logger

class Config(utils.logger.GenericLogger):
    def __init__(self, filename):
        self.config_file = os.path.join(os.getcwd(), "config", filename)
        super(Config, self).__init__()

    def load(self):
        self.logger.debug("Loading config %s " % self.config_file)
        with open(self.config_file) as f:
            self.config = yaml.load(f)

        return self.config

    def get(self, key):
        return self.config[key]
