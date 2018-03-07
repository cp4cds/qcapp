
import os
import datetime
import re
import glob
import json
import commands
from qc_settings import *

import utils
from esgf_dict import EsgfDict

CEDA_DATA_NODE = "esgf-data1.ceda.ac.uk"


class ESGFError(Exception):

    def __init__(self, message, dbobj, update=False):

        super(ESGFError, self).__init__(message)

        self.dbobj = dbobj
        self.update = update
        _log_message(dbobj, message)


def _log_message(dbobj, message, set_uptodate=False):

    """
        log_message

    A function that record a log or error message to a file.
    Additionally will record the same messsage to the database object and
    if "set_uptodate=True" is set it will update the database to reflect
    that a dataset or datafile record is correct.

    :param dbobj: A Django databse object, either Dataset or DataFile
    :param message: Log or error message string
    :param set_uptodate: Default False, if True will update the database

    """

    if set_uptodate:
        dbobj.up_to_date = True

    dbobj.up_to_date_note = message
    dbobj.save()


def _convert_version(iversion):
    """
        convert_version

    This function converts and ESGF dataset version from the from YYYYMMDD to a datetime object.

    If the ESGF version is not given in the form YYYYMMDD, and instead is provided in integer form, this will be
    returned unmodified.

    :param iversion: An ESGF version of the form YYYYMMDD (will support integer versions also)
    :return: [datetime_object] (exceptionally an integer)
    """

    if iversion == None:
        return iversion

    if len(iversion) == 8:
        oversion = datetime.datetime(int(iversion[0:4]), int(iversion[4:6]), int(iversion[6:8]))
    elif len(iversion) == 1:
        oversion = iversion


    return oversion


def _generate_latest_checksum_dict(node, version, cksum_type, cksum):
    """

        _generate_latest_checksum_dict

    Convert ESGF paramters of data-node (node), version, checksum_type and checksum into a dictionary format for ease of use

    :param node: ESGF data node
    :param version: ESGF version
    :param cksum_type: Checksum type, SHA256 or MD5
    :param cksum: Checksum

    :return: Checksum dictionary [Dict {'node':, 'version':, 'cksum_type': <SHA256/MD5>, 'cksum':}]
    """

    cksum_dict = {}
    cksum_dict['node'] = node
    cksum_dict['version'] = version
    cksum_dict['cksum_type'] = cksum_type
    cksum_dict['cksum'] = cksum

    return cksum_dict


def _read_json_cache_file(json_file):
    """
        read_datafile_json_cache

    Read cached ESGF JSON ouput
    :param json_file: Path to JSON file
    :return: JSON cached data [JSON dict]
    """

    try:
        json_data = open(json_file).read()
        _data = json.loads(json_data)
        json_resp = _data["response"]["docs"]

    except IOError:
        raise ESGFError("LATEST.000 [FAIL] :: NO JSON LOG FILE", df)

    return json_resp


def get_all_checksums(json_resp):
    """
        get_all_checksums

    A function that loops through all entries in a ESGF-json object and returns all the checksums
    at all the nodes and whether they are replica or master copies.

    :param json_resp: A ESGF query in JSON form
    :return: Checksums [dict]
             Format: {node: {'replica': T/F, 'version':, 'cksum_type': SHA256/MD5, 'cksum':}})
    """

    cksums = {}
    if len(json_resp) == 0:
        raise ESGFError("[FAIL] No Results for this query", df)

    for res in json_resp:
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


def get_latest_checksum(cksums):
    """
        get_latest_checksum

    From a dictionary object of checksums, determines if there is a valid latest checksum and returns it,
    else an error is recorded.

    :param cksums: A dictionary object of the form:
                   cksums[data_node] = {'replica':, 'version':, 'cksum_type':, 'cksum':}
    :return: latest_checksum [dict] {'node':, 'version':, 'cksum_type':, 'cksum':}
    """

    latest_checksum = {}
    latest_checksums = []
    versions = []
    n_masters = 0

    for key, values in cksums.items():
        # If record is a master record
        if values["replica"] == False:
            latest_checksums.append(_generate_latest_checksum_dict(key, values['version'], values['cksum_type'], values['cksum']))
            n_masters +=1
        versions.append(values['version'])

    if n_masters == 1:
        latest_checksum = latest_checksums[0]
        valid_latest_checksum = True
        return latest_checksum

    else:
        raise ESGFError("[ERROR] : No single master record, number of master records is {} : "
                        "JSON query ".format(n_masters), df)

    # elif n_masters == 0:
    #     if len(cksums.keys()) == 1:
    #
    #         if ".ceda." in cksums.keys()[0]:
    #             err_msg = "LATEST [WARN] :: No master record, CEDA hold only published copy"
    #             #_log_message(db_obj, logfile, "LATEST [WARN] :: No master record, CEDA hold only published copy")
    #             latest_checksum = _generate_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
    #             valid_latest_checksum = True
    #             return valid_latest_checksum, latest_checksum, err_msg
    #
    #         else:
    #             err_msg = "LATEST [FAIL] :: No master record, CEDA does not have a copy"
    #             #_log_message(db_obj, logfile, "LATEST [FAIL] :: No master record, CEDA does not have a copy")
    #             latest_checksum = _generate_latest_checksum_dict(None, None, None, None)
    #             valid_latest_checksum = False
    #             return valid_latest_checksum, latest_checksum, err_msg
    #
    #     elif len(cksums.keys()) > 1:
    #         for key in cksums.keys():
    #
    #             if "dkrz" in key:
    #                 err_msg = "LATEST [WARN] :: No master record, DKRZ checksum used a proxy for master"
    #                 # _log_message(db_obj, logfile, "LATEST [WARN] :: No master record, DKRZ checksum used a proxy for master")
    #                 valid_latest_checksum = True
    #                 latest_checksum =_generate_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
    #                 return valid_latest_checksum, latest_checksum, err_msg
    #
    #             elif "ceda" not in key:
    #                if not cksums[key]['cksum'] == "missing":
    #                    # _log_message(db_obj, logfile,
    #                    #             "LATEST [WARN] :: No master record, {} checksum used a proxy for master".format(key))
    #                    err_msg = "LATEST [WARN] :: No master record, {} checksum used a proxy for master".format(key)
    #                    valid_latest_checksum = True
    #                    latest_checksum = _generate_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
    #                    return valid_latest_checksum, latest_checksum, err_msg
    #
    #             else:
    #                 err_msg = "LATEST [WARN] :: No master data record checksums"
    #                 # _log_message(db_obj, logfile, "LATEST [WARN] :: No master data record checksums")
    #                 valid_latest_checksum = False
    #                 latest_checksum = _generate_latest_checksum_dict(None, None, None, None)
    #                 return valid_latest_checksum, latest_checksum, err_msg
    #
    # elif n_masters > 1:
    #     master_versions = []
    #
    #     for cks in latest_checksums:
    #         master_versions.append(cks['cksum'])
    #
    #     valid_master_latest_version, master_latest_version, err_msg = get_latest_version(db_obj, versions)
    #     master_latest_version = master_latest_version.strftime("%Y%m%d")
    #
    #     if err_msg != None:
    #         return False, _generate_latest_checksum_dict(None, None, None, None), err_msg
    #
    #     if valid_master_latest_version:
    #         for v in latest_checksums:
    #             if v['version'] == master_latest_version:
    #                 latest_checksum = v
    #                 valid_latest_checksum = True
    #                 return valid_latest_checksum, latest_checksum, err_msg
    #
    #     else:
    #         err_msg = "LATEST [ERROR]:: no valid master copy"
    #         # _log_message(db_obj, logfile, "LATEST [ERROR]:: no valid master copy")
    #         valid_latest_checksum = False
    #         latest_checksum = _generate_latest_checksum_dict(None, None, None, None)
    #         return valid_latest_checksum, latest_checksum, err_msg
    #


def compare_ceda_with_latest_cksum(ceda_version, latest_version):

    """

        compare_ceda_with_latest_cksum

    If the CEDA and latest versions are the same the function returns True, if they are different it returns False

    :param ceda_version: The CEDA dataset version for this datafile
    :param latest_version:
    :return: [Boolean]
    """

    if ceda_version == latest_version:
        return True

    if ceda_version != latest_version:
        error_message = "[ERROR] :: CEDA checksum is not the same as latest. CEDA version is: {}, " \
                        "LATEST checksum is: {}".format(ceda_version, latest_version)
        raise ESGFError(error_message, df)


def get_ceda_checksum(cksums):
    """
        get_ceda_checksum

    Get and return the CEDA checksum from a dictionary of checksums

    :param cksums: cksums[data_node] = {'replica':, 'version':, 'cksum_type':, 'cksum':}
    :return: Tuple (checksum [string], error_message)
    """
    try:
        ceda_cksum = cksums[CEDA_DATA_NODE]['cksum']

    except:
        raise ESGFError("[FATAL] unable to get a CEDA checksum", df)

    return ceda_cksum


def check_datafiles_are_latest(datasets, esgf_dict):
    """
        dataset_latest_check

    Runs test to check whether CP4CDS datafile is the most recent version on ESGF.

    :param datasets: A Django QuerySet of Dataset objects
    :param esgf_dict: An esgf_dict object
    """

    for ds in datasets:
        dfs = ds.datafile_set.all()
        check_datafile_is_latest(ds, dfs, esgf_dict)


def check_datafile_is_latest(ds, dfs, esgf_dict):

    try:
        global df
        for df in dfs:

            # if df.ncfile == "sim_OImon_MPI-ESM-LR_rcp26_r1i1p1_200601-210012.nc":
                df.up_to_date = False
                df.save()

                # Update the esgf_dict and get the cached json file (a distributed is latest ESGF query)
                esgf_dict, json_file = esgf_dict._generate_jsonfile_path(
                                        DATAFILE_LATEST_CACHE, ds, esgf_dict, "datafile", ncfile=df.ncfile)

                # Open and read cached JSON file
                json_resp = _read_json_cache_file(json_file)

                # checksums dictionary: {node: {'replica': T/F, 'version':, 'cksum_type': SHA256/MD5, 'cksum':}}
                checksums = get_all_checksums(json_resp)

                if CEDA_DATA_NODE not in checksums.keys():
                    raise ESGFError("[FATAL] :: File has no CEDA published record", df)

                # Check whether the CEDA published file is the most recent by checksum comparison
                latest_checksum = get_latest_checksum(checksums)

                ceda_checksum = get_ceda_checksum(checksums)

                ceda_cksum_is_latest = compare_ceda_with_latest_cksum(ceda_checksum, latest_checksum['cksum'])

                if ceda_cksum_is_latest:
                    success_message = "[PASS] : The CEDA datafile checksum {} is the same as the ESGF recorded latest " \
                                      "{}".format(ceda_checksum, latest_checksum['cksum'])
                    print(success_message)
                    _log_message(df, success_message)
                else:
                    raise ESGFError("[FAIL] Reached end of code and still didn't match checksum...", df)

    except ESGFError as e:
        print(str(e))

