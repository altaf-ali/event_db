import os
import yaml

import httplib
import requests
import urlparse

class DataverseAPI(object):
    CONFIG_YAML = "config.yaml"

    def __init__(self, config_path, config_file=CONFIG_YAML):
        self.config = yaml.load(open(os.path.join(config_path, config_file)))

    def url(self, *args):
        return urlparse.urljoin(self.config['url'], "/".join(args))

    def request(self, *endpoint):
        response = requests.get(self.url(*endpoint), params=self.config['params']).json()
        if response['status'] == 'OK':
            return response
        raise httplib.HTTPException

