import httplib
import requests
import urlparse

import utils.config

class DataverseAPI(object):
    def __init__(self, config_file, dataverse_key = "dataverse"):
        self.config = utils.config.Config(config_file)
        self.dataverse_config = self.config.load().get(dataverse_key)

    def url(self, *args):
        return urlparse.urljoin(self.dataverse_config['url'], "/".join(args))

    def request(self, *endpoint):
        response = requests.get(self.url(*endpoint), params=self.dataverse_config['params']).json()
        if response['status'] == 'OK':
            return response
        raise httplib.HTTPException

