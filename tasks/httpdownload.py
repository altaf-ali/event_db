import os
import requests

from contextlib import closing

import luigi

import utils.md5
import utils.logger

class HttpDownload(utils.logger.GenericLogger, luigi.Task):
    url = luigi.Parameter()
    target = luigi.Parameter()
    checksum = luigi.Parameter(default = 0)

    timeout = 10
    chunk_size = 512

    class IncompleteDownload(Exception):
        def __init__(self, url, size, target, downloaded):
            self.url = url
            self.size = size
            self.target = target
            self.downloaded = downloaded

        def __str__(self):
            return "Incomplete download: %s (%d) -> %s (%d)" % (self.url, self.size, self.target, self.downloaded)

    class ChecksumError(Exception):
        def __init__(self, url, target):
            self.url = url
            self.target = target

        def __str__(self):
            return "Checksum error: %s -> %s mismatch" % (self.url, self.target)

    def compare_checksum(self):
        if self.checksum == 0:
            self.logger.warn("Ignoring checksum, file = %s" % self.target)
            return True

        result = self.checksum == utils.md5.file_checksum(self.target, hex=True)
        if not result:
            self.logger.warn("Checksum mismatch, file=%s" % self.target)
        return result

    def complete(self):
        if self.output().exists() and self.compare_checksum():
            return luigi.Task.complete(self)
        return False

    def output(self):
        return luigi.LocalTarget(self.target)

    def download(self):
        self.logger.debug("Downloading %s from %s" % (self.output().fn, self.url))

        content_length = 0
        with closing(requests.get(self.url, stream=True, timeout=self.timeout)) as r, self.output().open("w") as f:
            content_length = int(r.headers['content-length'])
            for chunk in r.iter_content(chunk_size=self.chunk_size):
                if not chunk:
                    break
                f.write(chunk)
                f.flush()

        target_size = os.path.getsize(self.target)

        self.logger.debug("Download completed, %d of %d, target = %s" % (target_size, content_length, self.target))
        if not self.compare_checksum():
            raise HttpDownload.ChecksumError(self.url, self.target)

        if content_length and content_length != target_size:
            raise HttpDownload.IncompleteDownload(self.url, content_length, self.target, target_size)

    def run(self):
        caught_exceptions = (
            HttpDownload.IncompleteDownload,
            HttpDownload.ChecksumError,
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError
        )

        try:
            self.download()
        except caught_exceptions as e:
            self.logger.exception(e)

    def clean(self):
        pass

