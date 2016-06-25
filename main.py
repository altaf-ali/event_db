import luigi

from tasks.pipeline import Pipeline
import tasks.icews.icews
import tasks.ucdp.ged

class Main(Pipeline):
    def requires(self):
        pipelines = [
            tasks.ucdp.ged.GED_DatabaseWriter(pipeline = self),
            tasks.icews.icews.ICEWS_DatabaseWriter(pipeline = self)
        ]
        return pipelines

class Clean(luigi.Task):
    def complete(self):
        return False # forces clean to run

    def run(self):
        Main().clean()

if __name__ == "__main__":
    luigi.run()

