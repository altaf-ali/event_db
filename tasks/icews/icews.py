import glob

import os
import re
import zipfile

import pandas as pd

import luigi

from tasks.generic import GenericTask
from tasks.httpdownload import HttpDownload
from tasks.zip import ZipFileExtractor

import utils.config
import utils.database
import utils.dataverse
import utils.md5

class DatasetDownloader(GenericTask):
    ICEWS_CONFIG = "icews.yaml"
    DOWNLOADS_FOLDER = os.path.join(os.getcwd(), "downloads/icews")

    def requires(self):
        queue = list()

        dataverse = utils.dataverse.DataverseAPI(self.ICEWS_CONFIG)
        contents = dataverse.request("dataverses/icews/contents")

        for data in filter(lambda d: d['type']=='dataset', contents['data']):
            dataset_id = str(data['id'])
            dataset = dataverse.request("datasets", dataset_id)

            for f in dataset['data']['latestVersion']['files']:
                file_id = str(f['datafile']['id'])
                url = dataverse.url("access/datafile", file_id)

                filename = f['datafile']['name']
                checksum = f['datafile']['md5']
                target = os.path.join(self.DOWNLOADS_FOLDER, dataset_id, filename)

                if os.path.isfile(target) and utils.md5.file_checksum(target, hex=True) == checksum:
                    if zipfile.is_zipfile(target):
                        self.logger.debug("Queueing ZipFileExtractor, target = %s" % target)
                        queue.append(ZipFileExtractor(pipeline=self.pipeline, filename=target))
                else:
                    self.logger.debug("Queueing HttpDownload, target = %s" % target)
                    queue.append(HttpDownload(url, target, checksum))

        # now try to download the dataset
        yield queue

class DatasetCleaner(GenericTask):
    source = luigi.Parameter()
    target = luigi.Parameter()
    agent = luigi.Parameter(default=0)

    def output(self):
        return luigi.LocalTarget(self.target)

    def run(self):
        self.logger.debug("Cleaning dataset, source = %s" % self.source)
        events = pd.read_csv(self.source, index_col=0, sep='\t', encoding="utf-8")
        rename_axis = lambda x: x.replace(' ', '')
        events.index.rename(map(rename_axis, events.index.names)[0], inplace=True)
        events.rename(columns=rename_axis, inplace=True)

        with self.output().open("w") as f:
            events.to_csv(f, encoding="utf-8")

class DatasetBatchCleaner(GenericTask):
    target_folder = luigi.Parameter()

    def requires(self):
        return DatasetDownloader(pipeline=self.pipeline)

    def local_target(self, source_filename):
        match = re.match(r"^events\.(\d{4})\.(\d{14})\.tab$", os.path.basename(source_filename))
        target_filename = "events.%s.csv" % match.group(1)
        return luigi.LocalTarget(os.path.join(self.target_folder, target_filename))

    def sources(self):
        events_folder = os.path.join(DatasetDownloader.DOWNLOADS_FOLDER, "65874")
        return glob.glob(os.path.join(events_folder, "*.tab"))

    def output(self):
        return [self.local_target(f) for f in self.sources()]

    def dataset_cleaner(self, source, target):
        return DatasetCleaner(pipeline=self.pipeline, source=source, target=target)

    def run(self):
        filespec = zip(self.sources(), self.output())
        yield [self.dataset_cleaner(fs[0], fs[1].fn) for fs in filespec]

class ICEWS_DatabaseWriter(GenericTask):
    DB_CONFIG = "database.yaml"
    TABLE_NAME = "icews"
    DATASET_FOLDER = os.path.join(os.getcwd(), "datasets/icews")

    def requires(self):
        return DatasetBatchCleaner(pipeline=self.pipeline, target_folder=self.DATASET_FOLDER)

    def run(self):
        self.logger.debug("Connecting to database")
        config = utils.config.Config(self.DB_CONFIG)
        config.load()

        db = utils.database.Database(config.get('url'))
        db.connect()

        for i in self.input()[0:5]:
            self.logger.debug("Updating table, source = %s" % i.fn)
            #df = pd.read_csv(i.fn, index_col=0, encoding="utf-8")
            #db.write(self.TABLE_NAME, df)


