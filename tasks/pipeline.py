import os
import luigi

import pickle

from tasks.generic import GenericTask

class Pipeline(GenericTask):
    RESULTS_FOLDER = "results"

    pipeline = None  # override pipeline from GenericTask. It gets updated in __init__ to 'self'

    def __init__(self, *args, **kwargs):
        self.pipeline = self
        super(Pipeline, self).__init__(*args, **kwargs)

    @property
    def results_folder(self):
        return os.path.join(os.getcwd(), Pipeline.RESULTS_FOLDER, self.task_family)


