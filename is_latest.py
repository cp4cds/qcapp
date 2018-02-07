
import os
import datetime
import re
import glob
import json as jsn
from qc_settings import *

import utils
from esgf_dict import EsgfDict




def is_latest_dataset_cache(datasets, variable, esgf_dict):

    for ds in datasets:
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict, subdir=None, rw='w')
        url = esgf_dict.format_is_latest_dataset_url()
        esgf_dict.esgf_query(url, json_file)


def is_latest_datafile_cache(datasets, variable, esgf_dict):

    for ds in datasets:
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict, subdir="exper", rw='w')
        dfs = ds.datafile_set.all()

        for df in dfs:
            esgf_dict["ncfile"] = df.ncfile
            url = esgf_dict.format_is_latest_datafile_url()
            if esgf_dict["model"] == "CNRM-CM5":
                print url
            esgf_dict.esgf_query(url, json_file)


def get_all_versions(json_resp, versions, logfile, type):

    for d in json_resp:
        dataset_id = d["id"].split('|')[0]
        with open(logfile, 'w') as fw:
            fw.writelines("Checking {} is up to date :: {} \n".format(type, dataset_id))

        data_node = d["id"].split('|')[1]

        versions[data_node] = d["id"].split('|')[0].split('.')[-1].strip('v')

    return versions


def get_all_checksums(json_resp, cksums, logfile, type):

    for res in json_resp:
        dataset_id = res["id"].split('|')[0]
        with open(logfile, 'w') as fw:
            fw.writelines("Checking {} is up to date :: {} \n".format(type, dataset_id))

        data_node = res["id"].split('|')[1]
        version = res["dataset_id"].split('|')[0].split('.')[-1].strip('v')
        try:
            cksum = res["checksum"][0].strip()
        except KeyError:
            cksum = "Missing"

        try:
            cksum_type = res["checksum_type"][0].strip()
        except KeyError:
            cksum_type = "Unknown"
        replica = res["replica"]

        cksums[data_node] = {'replica': replica, 'version': version, 'cksum_type': cksum_type, 'cksum': cksum}

    return cksums


def convert_version(iversion):
    if len(iversion) == 8:
        oversion = datetime.datetime(int(iversion[0:4]), int(iversion[4:6]), int(iversion[6:8]))
    if len(iversion) == 1:
        oversion = iversion

    return oversion


def get_latest_version(db_obj, versions, logfile):
    # Get latest version/ Handles both v<YYYYMMDD> and v<N> formats

    dt_versions = []
    for version in versions:
        dt_versions.append(convert_version(version))

    try:
        latest_version = max(dt_versions)
        valid_latest_version = True

    except TypeError:
        errmsg = "LATEST.006 [FATAL] :: Cannot perform version_qc, no known latest version " \
                 "as types do not match {} \n".format(versions)
        db_obj.up_to_date_note = errmsg
        db_obj.save()
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        latest_version = None
        valid_latest_version = False

    return valid_latest_version, latest_version



def _get_latest_checksum(insts, versions, cksum_types, cksum):


    print insts, versions, cksum_types, cksum
    asdafasdfdsf
    valid_latest_cksum = None
    latest_cksum = None

    dt_versions = []
    for version in versions:
        dt_versions.append(convert_version(version))

    try:
        latest_version = max(dt_versions)
        valid_latest_version = True

    except TypeError:
        errmsg = "LATEST.006 [FATAL] :: Cannot perform version_qc, no known latest version " \
                 "as types do not match {} \n".format(versions)
        db_obj.up_to_date_note = errmsg
        db_obj.save()
        with open(logfile, 'a') as fw:
            fw.writelines("{} \n".format(errmsg))
        latest_version = None
        valid_latest_version = False

    return valid_latest_cksum, latest_cksum


def _get_latest_checksum_dict(node, version, cksum_type, cksum):
    cksum_dict = {}
    cksum_dict['node'] = node
    cksum_dict['version'] = version
    cksum_dict['cksum_type'] = cksum_type
    cksum_dict['cksum'] = cksum

    return cksum_dict


def log_message(dbobj, logfile, message, set_uptodate=False):

    with open(logfile, 'a') as fw:
        fw.writelines("{} \n".format(message))

    if set_uptodate:
        db_obj.up_to_date = True
    db_obj.up_to_date_note = message
    db_obj.save()


def get_latest_checksum(db_obj, cksums, logfile):
    """
    cksums[data_node] = {'replica': replica, 'version': version, 'cksum_type': cksum_type, 'cksum': cksum}

    :param db_obj:
    :param cksums:
    :param logfile:
    :return:
    """

    latest_checksum = {}
    latest_checksums = []
    versions = []
    n_masters = 0
    for key, values in cksums.items():
        if values["replica"] == False: # i.e. master record
            latest_checksums.append(_get_latest_checksum_dict(key, values['version'], values['cksum_type'], values['cksum']))
            n_masters +=1
        versions.append(values['version'])


    if len(latest_checksums) == 1:
        latest_checksum = latest_checksums[0]
        valid_latest_checksum = True
        return valid_latest_checksum, latest_checksum

    elif len(latest_checksums) > 1:
        master_versions = []

        for cks in latest_checksums:
            master_versions.append(cks['cksum'])

        valid_master_latest_version, master_latest_version = get_latest_version(db_obj, versions, logfile)
        master_latest_version = master_latest_version.strftime("%Y%m%d")

        if valid_master_latest_version:
            for v in latest_checksums:
                if v['version'] == master_latest_version:
                    latest_checksum = v
                    valid_latest_checksum = True
                    return valid_latest_checksum, latest_checksum
        else:
            log_message(db_obj, logfile, "LATEST [ERROR]:: no valid master copy")
            valid_latest_checksum = False
            latest_checksum = _get_latest_checksum_dict(None, None, None, None)
            return valid_latest_checksum, latest_checksum

    elif len(latest_checksums) == 0:
        log_message(db_obj, logfile, "LATEST [ERROR] :: no latest checksums")
        valid_latest_checksum = False
        latest_checksum = _get_latest_checksum_dict(None, None, None, None)
        return valid_latest_checksum, latest_checksum


def _check_published_and_db_versions_match(db_obj, ceda_publish_version_no, ceda_database_version_no, logfile):
    try:
        if ceda_database_version_no == ceda_publish_version_no:
            log_message(db_obj, logfile, "LATEST.003 [PASS] :: MATCH - CEDA database version {} and published " \
                        "ESGF version {} are the same".format(ceda_database_version_no, ceda_publish_version_no),
                        set_uptodate=True)
            return True

        if ceda_database_version_no != ceda_publish_version_no:
            log_message(db_obj, logfile, "LATEST.003 [ERROR] :: Mismatch between CEDA database version {} and " \
                                 "ESGF version {}".format(ceda_database_version_no, ceda_publish_version_no))
            return False

    except AttributeError:
         log_message(db_obj, logfile, "LATEST.004 [ERROR] :: CEDA database version unspecified")
         return False


def _check_published_and_db_checksums_match(db_obj, ceda_published_cksum, ceda_database_cksum, logfile):
    try:
        if ceda_database_cksum == ceda_published_cksum:
            log_message(db_obj, logfile, "LATEST.003 [PASS] :: MATCH - CEDA database version {} and published) " \
                        "ESGF version {} are the same".format(ceda_database_cksum, ceda_published_cksum),
                        set_uptodate=True)
            return True

        if ceda_database_cksum != ceda_published_cksum:
            log_message(db_obj, logfile, "LATEST.003 [ERROR] :: Mismatch between CEDA database version {} and " \
                                "ESGF version {}".format(ceda_database_cksum, ceda_published_cksum))
            return False

    except AttributeError:
        log_message(db_obj, logfile, "LATEST.004 [ERROR] :: CEDA database version unspecified")
        return False


def check_dataset_version(db_obj, versions, latest_version, ceda_data_node, logfile):
    ceda_published_version_no = versions[ceda_data_node]
    ceda_database_version_no = db_obj.version

    is_match_ceda_versions = _check_published_and_db_versions_match(db_obj, ceda_published_version_no,
                                                                    ceda_database_version_no, logfile)

    if is_match_ceda_versions:
        ceda_version = convert_version(ceda_published_version_no)
        ceda_version_is_latest = compare_ceda_with_latest_version(db_obj, ceda_version, latest_version, logfile)


def check_datafile_version(db_obj, all_cksums, latest_cksum, ceda_data_node, logfile):
    """

    :param db_obj:
    :param all_cksums: {{{'node': {'replica': Boolean, 'cksum_type': 'checksum type', 'version': 'version', 'cksum': 'checksum'}}
    :param latest_cksum: {'node': 'node', 'version': 'version', 'cksum_type': 'cheksum type', 'cksum': 'checksum'}
    :param ceda_data_node:
    :param logfile:
    :return:
    """

    ceda_published_checksum = all_cksums[ceda_data_node]['cksum']
    print "check data file version latsest checksum {}".format(latest_cksum)
    if latest_cksum['cksum_type'] == "SHA256":
        ceda_database_checksum = db_obj.sha256_checksum
    elif latest_cksum['cksum_type'] == "md5":
        ceda_database_checksum = db_obj.md5_checksum
    else:
        ceda_cksum_is_latest = False
        log_message(db_obj, logfile, "LATEST [ERROR] :: No valid checksum type")
        return ceda_cksum_is_latest

    is_match_ceda_cksums = _check_published_and_db_checksums_match(db_obj, ceda_published_checksum,
                                                                     ceda_database_checksum, logfile)

    if is_match_ceda_cksums:
        ceda_cksum_is_latest = compare_ceda_with_latest_cksum(db_obj, ceda_database_checksum, latest_cksum['cksum'], logfile)
        return ceda_cksum_is_latest
    else:
        ceda_cksum_is_latest = False
        return ceda_cksum_is_latest


def compare_ceda_with_latest_cksum(db_obj, ceda_version, latest_version, logfile):

    if ceda_version == latest_version:
        log_message(db_obj, logfile, "LATEST.000 [PASS] :: CEDA version is up to date at version: {}".format(latest_version), set_uptodate=True)
        return True


    if ceda_version != latest_version:
        log_message(db_obj, logfile, "LATEST.002 [ERROR] :: CEDA version is out of date. CEDA version is: {}, " \
                                    "LATEST version is: {}".format(ceda_version, latest_version))
        return False


def compare_ceda_with_latest_version(db_obj, ceda_version, latest_version, logfile):
    if ceda_version < latest_version:
        if isinstance(ceda_version, datetime.datetime): ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime): latest_version = latest_version.strftime("%Y%m%d")

        log_message(db_obj, logfile, "LATEST.002 [ERROR] :: CEDA version is out of date. CEDA version is: {}, "
                                     "LATEST version is: {}".format(ceda_version, latest_version))
        return False

    if ceda_version == latest_version:
        if isinstance(ceda_version, datetime.datetime): ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime): latest_version = latest_version.strftime("%Y%m%d")

        log_message(db_obj, logfile, "LATEST.000 [PASS] :: CEDA version is up to date at version: {}".format(latest_version),
                    set_uptodate=True)
        return True

    if ceda_version > latest_version:
        if isinstance(ceda_version, datetime.datetime):
            ceda_version = ceda_version.strftime("%Y%m%d")
        if isinstance(latest_version, datetime.datetime):
            latest_version = latest_version.strftime("%Y%m%d")

        log_message(db_obj, logfile, "LATEST.007 [FATAL] :: CEDA version {} can not be greater than "
                                     "latest version: {} \n".format(ceda_version, latest_version))
        return False


def dataset_latest_check(datasets, variable, esgf_dict):

    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False

    for ds in datasets:

        # Set up_to_date to be False as default will be overwritten to true if found to be true
        ds.up_to_date = False
        ds.save()
        # Open and read cached JSON file
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict, subdir=None, rw='r')
        json_data = open(json_file).read()
        _data = jsn.loads(json_data)
        json_resp = _data["response"]["docs"]


        logfile = os.path.join(DATASET_LATEST_DIR, os.path.basename(json_file).replace(".json", ".dataset.log"))

        # versions is a dictionary where the key is the datanode and value is the published version
        versions = {}
        versions = get_all_versions(json_resp, versions, logfile, type="dataset")

        if ceda_data_node not in versions.keys():
            log_message(dbobj, logfile, "LATEST.001 [ERROR] :: Dataset is missing from CEDA archive")
        else:
            valid_latest_version, latest_version = get_latest_version(ds, versions, logfile)
            if valid_latest_version:
                check_dataset_version(ds, versions, latest_version, ceda_data_node, logfile)


def datafile_latest_check(datasets, variable, esgf_dict):
    """
    For a given dataset test all datafiles are latest

    :return:
    """
    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False

    for ds in datasets:

        dfs = ds.datafile_set.all()

        for df in dfs:
            print df.archive_path
            df.up_to_date = False
            df.save()
            # Open and read cached JSON file
            esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict, subdir="exper")
            json_data = open(json_file).read()
            _data = jsn.loads(json_data)
            json_resp = _data["response"]["docs"]
            logfile = os.path.join(DATAFILE_LATEST_DIR, os.path.basename(json_file).replace(".json", ".datafile.log"))

            # versions is a dictionary where the key is the datanode and value is the published version
            checksums = {}
            checksums = get_all_checksums(json_resp, checksums, logfile, type="datafile")

            if ceda_data_node in checksums.keys():
                valid_latest_datafile, latest_checksum = get_latest_checksum(df, checksums, logfile)
                if valid_latest_datafile:
                    if isinstance(latest_checksum, dict):
                        ceda_cksum_is_latest = check_datafile_version(df, checksums, latest_checksum, ceda_data_node, logfile)
                    else:
                        print "WTF"
                        print checksums, latest_checksum, ceda_data_node
                else:
                    errmsg = "LATEST.009 [ERROR] :: No latest datafile found"
                    with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(errmsg))
            else:
                errmsg = "LATEST.001 [ERROR] :: Datafile is missing from CEDA archive"
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(errmsg))
                ceda_cksum_is_latest = False

            if ceda_cksum_is_latest:
                df.up_to_date = True
                df.save()
                logmsg = "LATEST.000 [PASS] :: CEDA datafile is up to date \n " \
                         "CEDA checksum :: {} \n " \
                         "LATEST checksum {} \n " \
                         "LATEST source {}".format(checksums[ceda_data_node]['cksum'],
                                                   latest_checksum['cksum'],
                                                   latest_checksum['node']
                                                   )
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(logmsg))
                print "SUCCESS a valid datafile was found for {}".format(df.ncfile)
            else:
                logmsg = "LATEST.009 [FAIL] :: CEDA datafile is not latest version \n " \
                         "CEDA checksum :: {} \n " \
                         "LATEST checksum {} \n " \
                         "LATEST source {}".format(checksums[ceda_data_node]['cksum'],
                                                   latest_checksum['cksum'],
                                                   latest_checksum['node']
                                                   )
                with open(logfile, 'a+') as fw: fw.writelines(" {} \n".format(logmsg))
                print "FAIL a valid datafile was not found or is missing for {}".format(df.ncfile)
                print logmsg
                url = utils._generate_datafile_url(df.ncfile)
                print url


