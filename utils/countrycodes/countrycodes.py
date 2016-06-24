import csv
import json
import os
import re
import tempfile

class CountryCodes(object):
    def __init__(self, filename=None, overwrite=False):
        self.cached = dict()
        self.iso3c = dict()
        self.iso3n = dict()
        self.countries = dict()

        self.jsonfilename = filename
        self.overwrite = overwrite

        if not self.jsonfilename:
            self.jsonfilename = tempfile.NamedTemporaryFile().name

        self._initialize()
        self.initialCount = len(self.countries)

    def __del__(self):
        self.save()

    def _initialize(self):
        if (os.path.isfile(self.jsonfilename)):
            with open(self.jsonfilename) as file:
                self.countries = json.load(file, object_hook=self._decode_dict)
        else:
            csvfilename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "countrycodes.csv")
            with open(csvfilename) as file:
                reader = csv.DictReader(file)
                for country in reader:
                    self.countries[country['country_name']] = country

        for name, country in self.countries.iteritems():
            self.iso3n[country['iso3n']] = country

        for name, country in self.countries.iteritems():
            self.iso3c[country['iso3c']] = country

    def _decode_list(self, data):
        rv = []
        for item in data:
            if isinstance(item, unicode):
                item = item.encode('utf-8')
            elif isinstance(item, list):
                item = self._decode_list(item)
            elif isinstance(item, dict):
                item = self._decode_dict(item)
            rv.append(item)
        return rv

    def _decode_dict(self, data):
        rv = {}
        for key, value in data.iteritems():
            if isinstance(key, unicode):
                key = key.encode('utf-8')
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            elif isinstance(value, list):
                value = self._decode_list(value)
            elif isinstance(value, dict):
                value = self._decode_dict(value)
            rv[key] = value
        return rv

    def find(self, name):
        cname = name.strip()
        if not cname:
            return None

        # try the easy way first
        if cname in self.countries:
            return self.countries[cname]

        for c in self.countries.values():
            if c['regex'] and re.search(c['regex'], cname, re.M|re.I):
                self.countries[cname] = c  # cache it for next time
                self.cached[cname] = c
                return c
        return None

    def findby_iso3c(self, code):
        return self.iso3c[str(code)]

    def findby_iso3n(self, code):
        return self.iso3n[str(code)]

    def save(self):
        if (self.overwrite or not os.path.isfile(self.jsonfilename)):
            with open(self.jsonfilename, "w") as file:
                json.dump(self.countries, file, ensure_ascii=False, indent=4)

    def dumpCache(self):
        print "CountryCodes cache"
        for key,value in self.cached.iteritems():
            print key, "=", value

