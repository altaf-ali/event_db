import logging

class GenericLogger(object):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        super(GenericLogger, self).__init__(*args, **kwargs)
