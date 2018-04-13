#!/usr/bin/env python
import django
django.setup()

import os
from sys import argv
from qcapp.models import *
import json
import requests
import commands
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import datetime
requests.packages.urllib3.disable_warnings()
ARCHIVE_ROOT = "/badc/cmip5/data/"
FILELIST = "ancil_files/cp4cds_all_vars.txt"
ALLEXPTS = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
JSONDIR = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/DATAFILE_CACHE"
class Cp4cds(object):

    def generate_search_url(self, variable, frequency, table, experiment):

        self.variable = variable
        self.frequency = frequency
        self.table = table
        self.experiment = experiment

        url_template = "https://esgf-index1.ceda.ac.uk/esg-search/search?type=File&project=CMIP5&" \
                       "variable={}&time_frequency={}&cmor_table={}&experiment={}&" \
                       "distrib=false&latest=true&format=application%2Fsolr%2Bjson&limit=10000"

        return url_template.format(variable, frequency, table, experiment)

    def run_esgf_json_query(self, url):

        self.url = url
        resp = requests.get(url, verify=False)
        json_resp = resp.json()
        return json_resp["response"]["docs"]



if __name__ == "__main__":
    with open(FILELIST) as fr:
        data = fr.readlines()

    nresults = 0
    for line in data:
        line = line.strip()
        variable, frequency, table = line.split(',')
        esgf_query = Cp4cds()
        for experiment in ALLEXPTS:
            url = esgf_query.generate_search_url(variable, frequency, table, experiment)
            df_results = esgf_query.run_esgf_json_query(url)
            json_filename = '.'.join([variable, frequency, table, experiment])
            json_file = os.path.join(JSONDIR, json_filename)
            with open(json_file, 'w') as fw:
                json.dump(df_results, fw)
            nresults += len(df_results)
            print(nresults)