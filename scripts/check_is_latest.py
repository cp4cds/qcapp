

from setup_django import *
import os
import sys
import datetime
import re
import glob
import json
import commands
from settings import *
import utils
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()


CEDA_DATA_NODE = "esgf-data1.ceda.ac.uk"
CEDA_INDEX_NODE = "esgf-index1.ceda.ac.uk"
DATAFILE_CACHE = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/distrib_datafile_json_cache"

distrib = True
latest = True

def _esgf_file_search(file):

    URL_TEMPLATE = "https://{node}/esg-search/search?type=File&project=CMIP5&title={ncfile}" \
                   "&distrib={distrib}&latest={latest}&format=application%2Fsolr%2Bjson&limit=100".format(
                    node=CEDA_INDEX_NODE, ncfile=file, distrib=distrib, latest=latest)

    return URL_TEMPLATE

def _make_cache_dir(dfobj):

    institute = dfobj.dataset.institute
    model = dfobj.dataset.model
    experiment = dfobj.dataset.experiment
    frequency = dfobj.dataset.frequency
    realm = dfobj.dataset.realm
    table = dfobj.dataset.cmor_table
    ensemble = dfobj.dataset.ensemble
    variable = dfobj.variable

    cache_dir = os.path.join(DATAFILE_CACHE, institute, model, experiment, frequency, realm, table, ensemble, variable)

    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir

def get_json_responses(url):

    resp = requests.get(url, verify=False)
    json_resp = resp.json()
    results = json_resp["response"]["docs"]
    return results



def cache_json_results(ncfile):

    for df in DataFile.objects.filter(ncfile=ncfile):

        json_cache_dir = _make_cache_dir(df)
        url = _esgf_file_search(df.ncfile)
        json_resp = utils.get_and_parse_json(url)
        json_log_file = os.path.join(json_cache_dir, ncfile.replace('.nc', '.json'))
        utils.write_json(json_log_file, json_resp)



def generate_json_cache(variable, frequency, table, experiment):

   for df in DataFile.objects.filter(variable=variable, dataset__cmor_table=table, dataset__frequency=frequency,
                                  dataset__experiment=experiment):
    # ncfile = 'tas_Amon_HadGEM2-ES_rcp45_r1i1p1_212412-214911.nc'
        cache_json_results(df.ncfile)


if __name__ == "__main__":
    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]
    experiment = sys.argv[4]

    generate_json_cache(variable, frequency, table, experiment)