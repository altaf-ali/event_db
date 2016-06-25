import os
import zipfile

import luigi

from tasks.generic import GenericTask

class ZipFileExtractor(GenericTask):
    filename = luigi.Parameter()
    target_folder = luigi.Parameter(default = None)

    def destination(self, z):
        if (self.target_folder != None):
            return self.target_folder

        if len(z.namelist()) > 1:
            return os.path.join(os.path.dirname(self.filename), os.path.splitext(os.path.basename(self.filename))[0])
        return os.path.dirname(self.filename)

    def output(self):
        items = []
        with zipfile.ZipFile(self.filename) as z:
            items = [luigi.LocalTarget(os.path.join(self.destination(z), i.filename)) for i in z.infolist()]
        return items

    def run(self):
        with zipfile.ZipFile(self.filename) as z:
            destination = self.destination(z)
            if not os.path.exists(destination):
                os.makedirs(destination)
            z.extractall(destination)
