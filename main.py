import luigi

from tasks.pipeline import Pipeline
from tasks.ucdp.ged import GED_DatabaseWriter
from tasks.icews.icews import ICEWS_DatabaseWriter

class Main(Pipeline):
    def requires(self):
        pipelines = [
            GED_DatabaseWriter(pipeline = self),
            ICEWS_DatabaseWriter(pipeline = self)
        ]
        return pipelines

class Clean(luigi.Task):
    def complete(self):
        return False # forces clean to run

    def run(self):
        Main().clean()

if __name__ == "__main__":
    luigi.run()

