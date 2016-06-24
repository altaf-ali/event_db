import datetime
import json
import os

import luigi

class Results(dict):
    def __init__(self, task):
        self.pipeline = task.pipeline
        self.task_id = task.task_id
        self.task_family = task.task_family
        filename = "%s.json" % (self.task_family)
        self.target = luigi.LocalTarget(os.path.join(self.pipeline.results_folder, filename))
        super(Results, self).__init__()

    def save(self, status):
        header = {
            'pipeline': self.pipeline.task_family,
            'task_id': self.task_id,
            'task_family': self.task_family,
            'timestamp': datetime.datetime.now().isoformat(),
            'status': status
        }
        self.update({ "header": header })

        with self.target.open("w") as f:
            json.dump(self, f, indent=4)
