import luigi

from tasks.pipeline import Pipeline
from tasks.generic import GenericTask

from pipelines.dataset_collector import DatasetCollectorPipeline

class Main(Pipeline):
    def requires(self):
        pipelines = [
            DatasetCollectorPipeline()
        ]
        return pipelines

class Clean(luigi.Task):
    def complete(self):
        return False # forces clean to run

    def run(self):
        Main().clean()

if __name__ == "__main__":
    luigi.run()

