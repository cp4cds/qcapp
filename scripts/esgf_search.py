#!/usr/bin/env python

from setup_django import *
from settings import *
from utils import *
import os
import sys
import json
import requests
import commands
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import datetime
requests.packages.urllib3.disable_warnings()


def find_all_local_datafiles(variable, frequency, table, experiment):

    distrib = "false"
    latest = "true"

    datafile_search_template = "https://{}/esg-search/search?type=File&project={}&" \
                               "variable={}&time_frequency={}&cmor_table={}&" \
                               "experiment={}&distrib={}&latest={}&" \
                               "format=application%2Fsolr%2Bjson&limit=10000"

    url = datafile_search_template.format(cedaindex, project, variable, frequency, table, experiment, distrib, latest)
    
    json_logdir, json_file = _define_local_json_cache_names(variable, frequency, table, experiment)
  
    if not os.path.exists(json_file):
        resp = requests.get(url, verify=False)
        json_resp = resp.json()

        with open(json_file, 'w') as filewrite:
            json.dump(json_resp, filewrite)


def esgf_search(variable, frequency, table):

    for experiment in ALLEXPTS:
        find_all_local_datafiles(variable, frequency, table, experiment)

if __name__ == "__main__":

    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]

    esgf_search(variable, frequency, table)
