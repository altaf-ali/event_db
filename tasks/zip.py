import os
import zipfile

import luigi

from tasks.generic import GenericTask

class ZipFileExtractor(GenericTask):
    filename = luigi.Parameter()

    def destination(self, filecount):
        if filecount > 1:
            return os.path.join(os.path.dirname(self.filename), os.path.basename(self.filename))
        return os.path.join(os.path.dirname(self.filename))

    def output(self):
        items = []
        with zipfile.ZipFile(self.filename) as z:
            items = [luigi.LocalTarget(os.path.join(self.destination(len(z.namelist())), i.filename)) for i in z.infolist()]
        return items

    def run(self):
        with zipfile.ZipFile(self.filename) as z:
            destination = self.destination(len(z.namelist()))
            if not os.path.exists(destination):
                os.makedirs(destination)
            z.extractall(destination)
