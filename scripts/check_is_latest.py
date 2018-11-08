

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


def esgf_file_search(file):

    URL_TEMPLATE = "https://{node}/esg-search/search?type=File&project=CMIP5&title={ncfile}" \
                   "&distrib={distrib}&format=application%2Fsolr%2Bjson&limit=100".format(
                    node=CEDA_INDEX_NODE, ncfile=file, distrib=distrib, latest=latest)

    return URL_TEMPLATE


def make_cache_dir(dfobj):

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

        json_cache_dir = make_cache_dir(df)
        url = esgf_file_search(df.ncfile)
        json_resp = utils.get_and_parse_json(url)
        json_log_file = os.path.join(json_cache_dir, ncfile.replace('.nc', '.json'))
        utils.write_json(json_log_file, json_resp)


def generate_json_cache(variable, frequency, table, experiment):

   for df in DataFile.objects.filter(variable=variable, dataset__cmor_table=table, dataset__frequency=frequency,
                                     dataset__experiment=experiment):
    # ncfile = 'tas_Amon_HadGEM2-ES_rcp45_r1i1p1_212412-214911.nc'
        cache_json_results(df.ncfile)


def read_json_cache_file(df, json_file):
    """
        read_datafile_json_cache
    Read cached ESGF JSON ouput
    :param json_file: Path to JSON file
    :return: JSON cached data [JSON dict]
    """

    try:
        json_data = open(json_file).read()
        json_resp = json.loads(json_data)
        # json_resp = _data["response"]["docs"]

    except IOError:
        print("FAIL")
        json_resp = None
        # raise ESGFError("IS_LATEST.000 [FAIL] :: NO JSON LOG FILE :: "
        #                 "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

    return json_resp


def check_datafile_is_latest(variable, frequency, table, experiment):

    for df in DataFile.objects.filter(variable=variable, dataset__cmor_table=table, dataset__frequency=frequency,
                                      dataset__experiment=experiment).exclude(dataset__supersedes__isnull=True):

        print("FILE : {}".format(df.ncfile))

        json_cache_dir = make_cache_dir(df)
        json_log_file = os.path.join(json_cache_dir, df.ncfile.replace('.nc', '.json'))
        json_resp = read_json_cache_file(df, json_log_file)

        if not json_resp:
            print("ERROR NO JSON FILE")

        else:
            file_info = {}
            for result in json_resp:
                node = result['data_node']

                file_info[node] = {
                                   'latest': result['latest'],
                                   'replica': result['replica'],
                                   'version': result['dataset_id'].split('|')[0].split('.')[-1],
                                   'checksum_type': result['checksum_type'],
                                   'checksum': result['checksum'],
                                   'tracking_id': result['tracking_id']
                                   }

            if CEDA_DATA_NODE not in file_info.keys():
                print "ERROR NO CEDA RECORD"

            else:

                latest_nodes = []
                for k, v in file_info.iteritems():
                    if v['latest']:
                        latest_nodes.append(k)

                latest_versions = set()
                for node in latest_nodes:
                    latest_versions.add(file_info[node]['version'])

                if len(latest_versions) == 1:
                    latest_version = latest_versions[0]
                else:
                    break

                if CEDA_DATA_NODE not in latest_nodes:

                    if file_info[CEDA_DATA_NODE]['checksum_type'] == file_info[latest_node]['checksum_type']:
                        if file_info[CEDA_DATA_NODE]['checksum'] == file_info[latest_node]['checksum']:
                            print("SAME FILE ALL OK")
                    elif file_info[la]:



                else:
                    print("CEDA HAS LATEST FILE BUT IN DIFFERENT CMIP5 DATASET VERSION")

            asdfasd



if __name__ == "__main__":
    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]
    experiment = sys.argv[4]

    # generate_json_cache(variable, frequency, table, experiment)
    check_datafile_is_latest(variable, frequency, table, experiment)
