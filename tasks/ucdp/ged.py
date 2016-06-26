import os
import glob

import pandas as pd

import luigi

from tasks.generic import GenericTask
from tasks.zip import ZipFileExtractor
from tasks.httpdownload import HttpDownload

import utils.config
import utils.database
import utils.dataverse
import utils.md5

class DatasetDownloader(GenericTask):
    target_folder = luigi.Parameter()

    UCDP_CONFIG = "ucdp.yaml"
    DOWNLOADS_FOLDER = os.path.join(os.getcwd(), "downloads/ucdp")

    config = utils.config.Config(UCDP_CONFIG)
    url = config.load().get('ged')['url']

    def output(self):
        return luigi.LocalTarget(os.path.join(self.DOWNLOADS_FOLDER, os.path.basename(self.url)))

    def requires(self):
        if not self.output().exists():
            self.logger.debug("Queueing HttpDownload, target = %s" % self.output().fn)
            yield HttpDownload(self.url, self.output().fn)


class DatasetExtractor(GenericTask):
    target_folder = luigi.Parameter()

    def requires(self):
        return DatasetDownloader(pipeline = self.pipeline, target_folder = self.target_folder)

    def run(self):
        yield ZipFileExtractor(pipeline = self.pipeline, filename = self.input().fn, target_folder = self.target_folder)

class GED_DatabaseWriter(GenericTask):
    DB_CONFIG = "database.yaml"
    TABLE_NAME = "ged"
    DATASET_FOLDER = os.path.join(os.getcwd(), "datasets/ucdp/ged")

    def requires(self):
        return DatasetExtractor(pipeline=self.pipeline, target_folder = self.DATASET_FOLDER)

    def run(self):
        self.logger.debug("Connecting to database")
        config = utils.config.Config(self.DB_CONFIG)
        config.load()

        db = utils.database.Database(config.get('url'))
        db.connect()

        files = glob.glob(os.path.join(self.DATASET_FOLDER, "ged*.csv"))
        for f in files:
            self.logger.debug("Updating table, source = %s" % f)
            df = pd.read_csv(f, index_col=0, encoding="utf-8")
            db.write(self.TABLE_NAME, df)



