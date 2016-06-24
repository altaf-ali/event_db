from tasks.pipeline import Pipeline
import tasks.icews as icews

class DatasetCollectorPipeline(Pipeline):
    def requires(self):
        return icews.DatasetCollector(pipeline=self)

    def run(self):
        pass
