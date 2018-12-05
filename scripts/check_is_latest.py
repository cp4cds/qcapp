
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
DO_NOT_PUBLISH_EQ_VERSION = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/donotpublish_eq_version.txt'
DO_NOT_PUBLISH_LTE_VERSION = '/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/qcapp/ancil_files/'
EXCLUDES = []
distrib = True
latest = True


def record_error(df, error_message):

    df.up_to_date = False
    df.up_to_date_note = error_message
    df.save()

    errorType = error_message.split(' :: ')[0]
    errorLevel = errorType.split(' ')[0].strip()
    log = esgf_file_search(df.ncfile, df.dataset.institute)
    qc_err, _ = QCerror.objects.get_or_create(file=df,
                                              check_type='LATEST',
                                              error_type=errorType,
                                              error_msg=error_message,
                                              error_level=errorLevel,
                                              report_filepath=log
                                              )

def create_excludes(file):

    with open(file) as r:
        dsids = r.readlines()

    add_to_excludes_list(dsids)

def add_to_excludes_list(dsids):
    for line in dsids:
        line = line.strip()
        if line.startswith('cmip5'):
            EXCLUDES.append(line)


def esgf_file_search(file, institute):

    URL_TEMPLATE = "https://{node}/esg-search/search?type=File&project=CMIP5&title={ncfile}" \
                   "&institute={institute}&distrib={distrib}&format=application%2Fsolr%2Bjson&limit=100".format(
                    node=CEDA_INDEX_NODE, ncfile=file, institute=institute, distrib=distrib, latest=latest)

    return URL_TEMPLATE


def get_or_make_cache_dir(dfobj):

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

        json_cache_dir = get_or_make_cache_dir(df)
        url = esgf_file_search(df.ncfile, df.dataset.institute)
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
        # print("FAIL")
        json_resp = None
        # raise ESGFError("IS_LATEST.000 [FAIL] :: NO JSON LOG FILE :: "
        #                 "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

    return json_resp


def convert_to_datetime_object(version_no):

    if version_no.startswith('v'):
        version_no = version_no.lstrip('v')
    print version_no
    if len(version_no) != 8:
        return int(version_no)
    else:
        try:
            return datetime.datetime(int(version_no[0:4]), int(version_no[4:6]), int(version_no[6:8]))
        except ValueError:
            return datetime.datetime(int(version_no[0:4]), 01, 01)

def calculate_latest_version(df, file_info):

    versions_dates = set()
    versions_ints = set()

    for value in file_info.itervalues():
        if isinstance(value['version_date'], datetime.datetime):
            versions_dates.add(value['version_date'])
        else:
            versions_ints.add(value['version_date'])

    if versions_dates:
        latest_version = max(versions_dates)
    else:
        latest_version = max(versions_ints)

    if versions_ints and versions_dates:

        try:
            time_dates = set()
            for value in file_info.itervalues():
                time_dates.add(value['timestamp'])
            latest_pub = max(time_dates)
            for value in file_info.itervalues():
                if value['timestamp'] == latest_pub:
                    latest_version = convert_to_datetime_object(value['version'])
        except:
             record_error("FATAL MIX OF VERSION TYPES :: {} {}".format(df.ncfile, esgf_file_search(df.ncfile, df.dataset.institute)))


    return latest_version


def get_latest_details(file_info, latest_version):

    for k, v in file_info.iteritems():
        if v['version_date'] == latest_version:
            if not v['replica']:
                return v['checksum'], v['checksum_type'], v['urls']

            return v['checksum'], v['checksum_type'], v['urls'][0]

    return None, None, None


def compare_checksums(ceda, latest):
    
    if ceda == latest:
        return True
    else:
        return False


def get_json_data(df, variable, frequency, table, experiment):

    json_cache_dir = get_or_make_cache_dir(df)
    json_log_file = os.path.join(json_cache_dir, df.ncfile.replace('.nc', '.json'))
    json_resp = read_json_cache_file(df, json_log_file)

    if json_resp:
        return json_log_file, json_resp

    else:
        record_error(df, "FATAL NO JSON RESPONSE :: {}".format(esgf_file_search(df.ncfile, df.dataset.institute)))

    return json_log_file, None


def generate_file_info(json_resp):

    file_info = {}
    for result in json_resp:
        node = result['data_node']
        version_date = convert_to_datetime_object(result['dataset_id'].split('|')[0].split('.')[-1])
        time = convert_to_datetime_object(''.join(result['_timestamp'][:10].split('-')))

        try: cksumType = result['checksum_type'][0]
        except(KeyError): cksumType = 'unknown'

        try: cksum = result['checksum'][0]
        except(KeyError): cksum = 'unknown'

        file_info[node] = {
            'latest': result['latest'],
            'replica': result['replica'],
            'version': result['dataset_id'].split('|')[0].split('.')[-1],
            'version_date': version_date,
            'checksum_type': cksumType,
            'checksum': cksum,
            'tracking_id': result['tracking_id'][0],
            'timestamp': time,
            'urls': result['url']
        }

    return file_info



def check_ceda_is_latest_not_published(df, latest_version, latest_checksum_type, latest_checksum):

    print "No ceda record {}".format(df.ncfile)

    _a = '.'.join(df.dataset.dataset_id.split('.')[:-2])
    _b = df.dataset.dataset_id.split('.')[-1]
    cmip5_dsid = '.'.join([_a, _b])

    if cmip5_dsid in EXCLUDES:
        print("EXCLUDED")
        return False

    try:
        ceda_version = convert_to_datetime_object(os.readlink(os.path.dirname(df.gws_path)))
    except:
        ceda_version = None

    if ceda_version == latest_version:
        return True

    else:
        if latest_checksum_type == 'SHA256':
            if df.sha256_checksum == 'sha256sum:' or not df.sha256_checksum:
                df.sha256_checksum = commands.getoutput('sha256sum ' + df.gws_path).split(' ')[0]
                df.save()
            try:
                ceda_sha256 = df.sha256_checksum
            except:
                ceda_sha256 = None
            if ceda_sha256 == latest_checksum:
                return True

        if latest_checksum_type == 'MD5':
            try:
                ceda_md5 = df.md5_checksum
            except:
                ceda_md5 = None
            if ceda_md5 == latest_checksum:
                return True


    return False



def check_ceda_is_latest(df, file_info, latest_version, latest_checksum_type, latest_checksum):


    if file_info[CEDA_DATA_NODE]['version_date'] == latest_version:
        return True

    if latest_checksum_type == file_info[CEDA_DATA_NODE]['checksum_type']:
        return compare_checksums(file_info[CEDA_DATA_NODE]['checksum'], latest_checksum)

    elif latest_checksum_type == 'md5' or latest_checksum_type == "MD5":
        return compare_checksums(df.md5_checksum, latest_checksum)

    elif latest_checksum_type == 'sha256' or latest_checksum_type == "SHA256":
        return compare_checksums(df.md5_checksum, latest_checksum)

    else:
        return False



def check_datafile_is_latest(variable, frequency, table, experiment):


    for df in DataFile.objects.filter(variable=variable, dataset__cmor_table=table, dataset__frequency=frequency,
                                      dataset__experiment=experiment):
        df.up_to_date = None
        df.up_to_date_note = None
        df.save()

        print df.ncfile

        latest_error = df.qcerror_set.filter(check_type='LATEST').first()
        if latest_error:
            latest_error.delete()

        json_log_file, json_resp = get_json_data(df, variable, frequency, table, experiment)

        if json_resp:

            file_info = generate_file_info(json_resp)
            latest_version = calculate_latest_version(df, file_info)
            latest_checksum, latest_checksum_type, latest_urls = get_latest_details(file_info, latest_version)

            if CEDA_DATA_NODE not in file_info.keys():
                record_error(df, "FATAL NO CEDA RECORD :: {} {}".format(df.ncfile, esgf_file_search(df.ncfile, df.dataset.institute)) )
                # ceda_is_lateset = check_ceda_is_latest_not_published(df, latest_version, latest_checksum_type, latest_checksum)
                #
                # if not ceda_is_lateset:
                #     print "FATAL NO CEDA RECORD {} {}".format(df.ncfile, esgf_file_search(df.ncfile, df.dataset.institute))
                #     continue
                # else:
                #     print("WARNING: No published record of CEDA replica {}".format(esgf_file_search(df.ncfile, df.dataset.institute)))

            else:
                ceda_is_lateset = check_ceda_is_latest(df, file_info, latest_version, latest_checksum_type, latest_checksum)

                if not ceda_is_lateset:
                    record_error(df, "UPDATE CEDA NOT LATEST :: {} {} : DOWNLOAD LINKS : {}".format(df.ncfile,
                                                                                esgf_file_search(df.ncfile, df.dataset.institute),
                                                                                latest_urls) )
                else:
                    df.up_to_date = True
                    df.save()


if __name__ == "__main__":

    # create_excludes(DO_NOT_PUBLISH_EQ_VERSION)
    # create_excludes(DO_NOT_PUBLISH_LTE_VERSION)

    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]
    # experiment = sys.argv[4]

    # generate_json_cache(variable, frequency, table, experiment)
    for experiment in ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
        check_datafile_is_latest(variable, frequency, table, experiment)