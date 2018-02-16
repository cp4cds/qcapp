
import os
import datetime
import re
import glob
import json as jsn
import commands
from qc_settings import *

import utils
from esgf_dict import EsgfDict


def is_latest_dataset_cache(datasets, esgf_dict):
    """
        is_latest_dataset_cache

    This function loops of a QuerySet of ESGF-type datasets.
    It uses the esgf_dict object to construct an output file path for a json file
    It constructs an esgf query string
    It makes a call to ESGF and executes the query the results are cached as JSON files


    :param datasets: A Django models QuerySet
    :param esgf_dict: An esgf_dict object
    :return: None
    """

    for ds in datasets:
        esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict, rw='w')
        url = esgf_dict.format_is_latest_dataset_url()
        esgf_dict.esgf_query(url, json_file)


def is_latest_datafile_cache(datasets, esgf_dict):
    """
        is_latest_datafile_cache

    This function loops of a QuerySet of ESGF-type datasets and all the datafiles that
    exist within that dataset.
    It uses the esgf_dict object to construct an output file path for a json file.
    It constructs an esgf query string
    It makes a call to ESGF and executes the query the results are cached as JSON files


    :param datasets: A Django models QuerySet
    :param esgf_dict: An esgf_dict object
    :return: None

    :return:
    """


    for ds in datasets:
        dfs = ds.datafile_set.all()

        for df in dfs:
            esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict,
                                                                    subdir="exper", rw='w', ncfile=df.ncfile)
            esgf_dict["ncfile"] = df.ncfile
            url = esgf_dict.format_is_latest_datafile_url()
            esgf_dict.esgf_query(url, json_file)


def get_all_versions(json_resp, versions, logfile):
    """
        get_all_versions

    A function that loops through all entries in a ESGF-json object and returns all the versions
    at all the nodes and whether they are replica or master copies.

    :param json_resp: A ESGF query in JSON form
    :param versions: a dictonary object in the form {node: <data_node> {replica: <True/False>, version: <version_number>}}
    :param logfile: A logfile for writing WARNINGS and ERROR messages

    :return: [Dict: {node: <data_node> {replica: <True/False>, version: <version_number>}}]
    """

    for res in json_resp:

        dataset_id = res["id"].split('|')[0]
        with open(logfile, 'w') as fw:
            fw.writelines("Checking dataset is up to date :: {} \n".format(dataset_id))

        data_node = res["id"].split('|')[1]
        version = res["id"].split('|')[0].split('.')[-1].strip('v')
        replica = res["replica"]

        versions[data_node] = {'replica': replica, 'version': version}

    return versions


def get_all_checksums(json_resp, cksums, logfile):
    """
        get_all_checksums

    A function that loops through all entries in a ESGF-json object and returns all the checksums
    at all the nodes and whether they are replica or master copies.


    :param json_resp: A ESGF query in JSON form
    :param cksums:  A checksums dictionary object of the form
           {node: <data_node> {'replica': T/F, 'version': version_no, 'cksum_type': SHA256/MD5, 'cksum': checksum}}
    :param logfile: A logfile for writing WARNINGS and ERROR messages

    :return: [Dict {node: <data_node> {'replica': T/F, 'version': version_no, 'cksum_type': SHA256/MD5, 'cksum': checksum}}]
    """

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
    """
        convert_version

    This function converts and ESGF dataset version from the from YYYYMMDD to a datetime object.

    If the ESGF version is not given in the form YYYYMMDD, and instead is provided in integer form, this will be
    returned unmodified.

    :param iversion: An ESGF version of the form YYYYMMDD (will support integer versions also)
    :return: [datetime_object] (exceptionally an integer)
    """

    if len(iversion) == 8:
        oversion = datetime.datetime(int(iversion[0:4]), int(iversion[4:6]), int(iversion[6:8]))
    if len(iversion) == 1:
        oversion = iversion

    return oversion


def get_latest_version(db_obj, versions, logfile):
    """
        get_latest_version

    Using the versions obtained from "get_all_versions" this function loops through each version and where the
    version number is of the YYYYMMDD form using the "convert_version" function compares all the published versions
    and obtains the latest.

    Presently does not seem to support ESGF published versions in the integer form.

    :param db_obj: A Django database object
    :param versions: A dictionary object of the form {node: <data_node> {replica: <True/False>, version: <version_number>}}
    :param logfile: A logfile for writing WARNINGS and ERROR messages

    :return: tuple of valid_latest_version and the version number [Boolean, version_number]
    """

    dt_versions = []
    for version in versions:
        dt_versions.append(convert_version(version))

    try:
        latest_version = max(dt_versions)
        valid_latest_version = True

    except TypeError:
        log_message(db_obj, logfile, "LATEST.006 [FATAL] :: Cannot perform version_qc, no known latest version " \
                 "as types do not match {} ".format(versions))
        latest_version = None
        valid_latest_version = False

    return valid_latest_version, latest_version


def _get_latest_checksum_dict(node, version, cksum_type, cksum):
    """

        _get_latest_checksum_dict

    Convert ESGF paramters of data-node (node), version, checksum_type and checksum into a dictionary format for ease of use

    :param node: ESGF data node
    :param version: ESGF version
    :param cksum_type: Checksum type, SHA256 or MD5
    :param cksum: Checksum

    :return: Checksum dictionary [Dict {node: <data_node>, version: <version>, cksum_type: <SHA256/MD5>, cksum: <checksum>}]
    """

    cksum_dict = {}
    cksum_dict['node'] = node
    cksum_dict['version'] = version
    cksum_dict['cksum_type'] = cksum_type
    cksum_dict['cksum'] = cksum

    return cksum_dict


def log_message(dbobj, logfile, message, set_uptodate=False):

    """
        log_message

    A function that record a log or error message to a file.
    Additionally will record the same messsage to the database object and
    if "set_uptodate=True" is set it will update the database to reflect
    that a dataset or datafile record is correct.

    :param dbobj: A Django databse object, either Dataset or DataFile
    :param logfile: A logfile for recording log or error messages.
    :param message: Log or error message string
    :param set_uptodate: Default False, if True will update the database

    """

    with open(logfile, 'a') as fw:
        fw.writelines("{} \n".format(message))

    if set_uptodate:
        dbobj.up_to_date = True

    dbobj.up_to_date_note = message
    dbobj.save()


def get_latest_checksum(db_obj, cksums, logfile):
    """

        get_latest_checksum

    From a dictionary object of checksums, determines if there is a valid latest checksum and returns it,
    else an error is recorded.

    :param db_obj: A Django database object, here a DataFile
    :param cksums: A dictionary object of the form:
                   cksums[data_node] = {'replica': replica, 'version': version, 'cksum_type': cksum_type, 'cksum': cksum}
    :param logfile: A logfile for recording log or error messages.

    :return: Returns a tuple of valid_latest_checksum [Bool], latest_checksum [string]
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

    if n_masters == 1:
        latest_checksum = latest_checksums[0]
        valid_latest_checksum = True
        return valid_latest_checksum, latest_checksum

    elif n_masters == 0:
        if len(cksums.keys()) == 1:

            if ".ceda." in cksums.keys()[0]:
                log_message(db_obj, logfile, "LATEST [WARN] :: No master record, CEDA hold only published copy",
                            set_uptodate=True)
                latest_checksum = _get_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
                valid_latest_checksum = True
                return valid_latest_checksum, latest_checksum

            else:
                log_message(db_obj, logfile, "LATEST [FAIL] :: No master record, CEDA does not have a copy")
                latest_checksum = _get_latest_checksum_dict(None, None, None, None)
                valid_latest_checksum = False
                return valid_latest_checksum, latest_checksum

        elif len(cksums.keys()) > 1:
            for key in cksums.keys():

                if "dkrz" in key:
                    log_message(db_obj, logfile, "LATEST [WARN] :: No master record, DKRZ checksum used a proxy for master", set_uptodate=True)
                    valid_latest_checksum = True
                    latest_checksum = _get_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
                    return valid_latest_checksum, latest_checksum

                elif "ceda" not in key:
                   if not cksums[key]['cksum'] == "missing":
                       log_message(db_obj, logfile,
                                   "LATEST [WARN] :: No master record, {} checksum used a proxy for master".format(key),
                                   set_uptodate=True)
                       valid_latest_checksum = True
                       latest_checksum = _get_latest_checksum_dict(key, cksums[key]['version'], cksums[key]['cksum_type'], cksums[key]['cksum'])
                       return valid_latest_checksum, latest_checksum

                else:
                    log_message(db_obj, logfile, "LATEST [WARN] :: No master data record checksums")
                    valid_latest_checksum = False
                    latest_checksum = _get_latest_checksum_dict(None, None, None, None)
                    return valid_latest_checksum, latest_checksum

    elif n_masters > 1:
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


def _check_published_and_db_versions_match(db_obj, ceda_publish_version_no, ceda_database_version_no, logfile):

    """

        _check_published_and_db_versions_match

    Compares the CEDA published ESGF version with the CP4CDS database version, if they are the same returns True.

    False is returned and an error message recorded if
    * the versions do not match
    * no version is record in the CP4CDS database or published to ESGF

    :param db_obj: A Django database object, here a Dataset
    :param ceda_publish_version_no: The CEDA ESGF published dataset version
    :param ceda_database_version_no: The CP4CDS Django database recorded version
    :param logfile: A logfile for log or error messages

    :return: [Boolean]
    """


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


def check_dataset_version(db_obj, versions, latest_version, ceda_data_node, logfile):

    """

        check_dataset_version

    This function checks whether the CEDA dataset version is the most recent and returns True if it is.

    :param db_obj: A Django database object, here a Dataset
    :param versions: A dictionary of versions in the form
           ???????????????????????
    :param latest_version: The latest version
    :param ceda_data_node: [string] of ceda data node
    :param logfile: A file to record log or error messages

    :return: Returns True if the CEDA dataset version is the most recent [Boolean]
    """

    ceda_published_version_no = versions[ceda_data_node]['version']
    ceda_database_version_no = db_obj.version

    is_match_ceda_versions = _check_published_and_db_versions_match(db_obj, ceda_published_version_no,
                                                                    ceda_database_version_no, logfile)

    if is_match_ceda_versions:
        ceda_version = convert_version(ceda_published_version_no)
        dt_latest_version = convert_version(latest_version)
        ceda_version_is_latest = compare_ceda_with_latest_version(db_obj, ceda_version, dt_latest_version, logfile)
    else:
        ceda_version_is_latest = False

    return ceda_version_is_latest

def check_datafile_version_and_checksum(db_obj, all_cksums, latest_cksum, ceda_data_node, logfile):
    """

        check_datafile_version_and_checksum

    This function checks whether the CEDA checksum is the lastest

    List of checks performed:
    *
    *
    *

    :param db_obj: A database object; here DataFile
    :param all_cksums: {{'node': {'replica': Boolean, 'cksum_type': 'checksum type', 'version': 'version', 'cksum': 'checksum'}}
    :param latest_cksum: {'node': 'node', 'version': 'version', 'cksum_type': 'cheksum type', 'cksum': 'checksum'}
    :param ceda_data_node:
    :param logfile:

    :return: True if CEDA checksum is same as the lastest [Boolean]
    """
    ceda_published_checksum = all_cksums[ceda_data_node]['cksum']

    if latest_cksum['cksum_type'] == "SHA256":
        valid_checksum_type = "SHA256"
        ceda_database_checksum = db_obj.sha256_checksum
    elif latest_cksum['cksum_type'] == "MD5" or latest_cksum['cksum_type'] == "md5":
        valid_checksum_type = "MD5"
        ceda_database_checksum = db_obj.md5_checksum
    else:
        log_message(db_obj, logfile, "LATEST [ERROR] :: No valid checksum type")
        return False

    if valid_checksum_type == "SHA256":

        if latest_cksum['cksum'] == db_obj.sha256_checksum:
            log_message(db_obj, logfile,
                        "LATEST [PASS] :: Checksum of CEDA file {} and latest published checksum {} match".
                        format(ceda_database_checksum, latest_cksum['cksum']), set_uptodate=True)
            return True
        else:
            log_message(db_obj, logfile,
                        "LATEST [FAIL] :: Checksum of CEDA file {} and latest published checksum {} DO NOT match".
                        format(ceda_database_checksum, latest_cksum['cksum']))
            return False

    elif valid_checksum_type == "MD5":
        if latest_cksum['cksum'] == db_obj.md5_checksum:
            log_message(db_obj, logfile,
                        "LATEST [PASS] :: Checksum of CEDA file {} and latest published checksum {} match".
                        format(ceda_database_checksum, latest_cksum['cksum']), set_uptodate=True)
            return True
        else:
            log_message(db_obj, logfile,
                        "LATEST [FAIL] :: Checksum of CEDA file {} and latest published checksum {} DO NOT match".
                        format(ceda_database_checksum, latest_cksum['cksum']))
            return False

    else:
        log_message(db_obj, logfile, "LATEST [ERROR] :: Cannot compare checksums")
        return False


def compare_ceda_with_latest_cksum(db_obj, ceda_version, latest_version, logfile):

    """

        compare_ceda_with_latest_cksum

    If the CEDA and latest versions are the same the function returns True, if they are different it returns False

    :param db_obj: A Django database object; here Datafile
    :param ceda_version: The CEDA dataset version for this datafile
    :param latest_version:
    :param logfile: A logfile for recording log or error messages.

    :return: [Boolean]
    """

    if ceda_version == latest_version:
        log_message(db_obj, logfile, "LATEST.000 [PASS] :: CEDA version is up to date at version: {}".format(latest_version), set_uptodate=True)
        return True


    if ceda_version != latest_version:
        log_message(db_obj, logfile, "LATEST.002 [ERROR] :: CEDA version is out of date. CEDA version is: {}, " \
                                    "LATEST version is: {}".format(ceda_version, latest_version))
        return False


def compare_ceda_with_latest_version(db_obj, ceda_version, latest_version, logfile):

    """

        compare_ceda_with_latest_version

    This function compares the CEDA dataset version with the latest version and returns True if they are the same.

    This function first checks to ensure that the version types match, i.e in case one version is in the YYYYMMDD
    format and one in the integer format.

    Where the versions don't match the function returns False

    :param db_obj: Django database object; Dataset
    :param ceda_version: The CEDA dataset version
    :param latest_version: The valid latest version
    :param logfile: A logfile for recording log or error messages

    :return: [Boolean]
    """

    # Need to record this as the final entry in db
    if type(ceda_version) != type(latest_version):
        log_message(db_obj, logfile, "LATEST.000 [FATAL] :: INCONSTENT VERSION FORMATS CEDA version {} can not be compared to  "
                                     "latest version: {} ".format(ceda_version, latest_version))
        return False

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
                                     "latest version: {} ".format(ceda_version, latest_version))
        return False


def update_db_checksums(dbobj, checksums, ceda_data_node):

    """

        update_db_checksums

    This function updates the database checksums.

    :param dbobj: Django database object; DataFile
    :param checksums: Dictionary object of checksums in the form:
                    {node: <data_node> { ???? cksum_type: SHA256/MD5, checksum: <checksum>}}
    :param ceda_data_node: String defining the ceda data node

    :return:
    """

    if checksums[ceda_data_node]["cksum_type"] == "SHA256":
        ceda_published_cksum = checksums[ceda_data_node]["cksum"]
        dbobj.sha256_checksum = ceda_published_cksum
      #  dbobj.md5_checksum = commands.getoutput('md5sum ' + dbobj.archive_path).split(' ')[0]
    elif checksums[ceda_data_node]["cksum_type"] == "MD5" or checksums[ceda_data_node]["cksum_type"] == "md5":
        ceda_published_cksum = checksums[ceda_data_node]["cksum"]
        dbobj.md5_checksum = ceda_published_cksum
    else:
        dbobj.sha256_checksum = commands.getoutput('sha256sum ' + dbobj.archive_path).split(' ')[0]
        dbobj.md5_checksum = commands.getoutput('md5sum ' + dbobj.archive_path).split(' ')[0]

    dbobj.save()


def read_datafile_json_cache(dbobj, json_file, logfile):

    """

        read_datafile_json_cache

    Read cached ESGF JSON ouput

    :param dbobj: Django database object; Dataset or DataFile
    :param json_file: Path to JSON file
    :param logfile: Path to logfile for recording log or error messages

    :return: JSON response information [JSON dict]
    """

    try:
        json_data = open(json_file).read()
        _data = jsn.loads(json_data)
        json_resp = _data["response"]["docs"]

    except IOError:
        log_message(dbobj, logfile, "LATEST.000 [FAIL] :: NO JSON LOG FILE")
        json_resp = {}

    return json_resp


def get_lastest_version(ds, versions, logfile):

    """

        get_lastest_version

    This function obtains the latest dataset version.
    It firstly looks for a valid master version, i.e. where there is only one master version.
    If one master version is found this is used as the valid latest version number.

    If no master version is found, an alternative proxy master with a latest date is searched for.

    If multiple masters are found the master version with the latest version is used.

    :param ds: Django database object; dataset
    :param versions: Dictionary of versions in the form
            {node: <data_node> {replica: True/False, version: <version_number>} }
    :param logfile: Path to logfile for log or error messages.

    :return: Tuple of if version is valid and what the version is ([Boolean], [String])
    """

    master_versions = {}
    n_masters = 0
    for node, values in versions.items():
        if values['replica'] == False:
            master_versions[node] = values['version']
            n_masters += 1

    if n_masters == 0:
        log_message(ds, logfile, "LATEST :: [WARN] :: No master versions")
        valid_master_version, master_version = get_alternative_version(ds, versions, logfile)

    elif n_masters == 1:
        valid_master_version = True
        master_version = values['version']
        log_message(ds, logfile, "LATEST :: [PASS] :: Valid master version found {}, {}".format(node, values['version']))

    # multiple masters
    else:
        log_message(ds, logfile, "LATEST :: [WARN] :: Multiple master versions")
        valid_master_version, master_version = get_alternative_version(ds, versions, logfile)

    return valid_master_version, master_version

def get_alternative_version(ds, versions, logfile):

    """

        get_alternative_version

    When no master versions are found an alternative "proxy" master version is sought.

    DKRZ is used as a trusted site to initaly search for a published version number,
    if a master doesn't exist there any site is used and a WARNING message is raised.

    :param ds: A django database object; dataset
    :param versions: Dictionary of versions in the form:
                    {node: <data_node> {replica: True/False, version: <version_number>} }
    :param logfile: Path to logfile for recording log or error messages.

    :return: Tuple of if version is valid and what the version is ([Boolean], [String])
    """

    if len(versions.keys()) == 0:
        log_message(ds, logfile, "LATEST :: [FAIL] :: No versions")
        return False, None

    elif len(versions.keys()) == 1:
        for node in versions.keys():
            if "ceda" in node:
                log_message(ds, logfile, "LATEST :: [WARN] :: Only version is CEDA")
                valid_master_version = True
                master_version = versions[node]['version']

            else:
                log_message(ds, logfile, "LATEST :: [FAIL] :: No CEDA version")
                valid_master_version = False
                master_version = None

        return valid_master_version, master_version

    else: # More than 1
        for node in versions.keys():
            if "dkrz" in node:
                log_message(ds, logfile, "LATEST :: [WARN] :: DKRZ version is proxy for master version")
                valid_master_version = True
                master_version = versions[node]['version']
            elif "ceda" not in node:
                master_version = versions[node]['version']
                valid_master_version = True
                log_message(ds, logfile, "LATEST :: [WARN] :: Proxy master version used {}".format(versions[node]))
            else:
                log_message(ds, logfile, "LATEST :: [FAIL] :: No master version found")
                valid_master_version = False
                master_version = None

            return valid_master_version, master_version


def dataset_latest_check(datasets, esgf_dict):

    """
        dataset_latest_check

    Driver to call functions to test whether the dataset version at CEDA is the latest.

    :param datasets: A Django QuerySet of Dataset objects
    :param esgf_dict: An esgf_dict object
    """

    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False

    for ds in datasets:
        # if ds.esgf_drs == "cmip5.output1.CSIRO-BOM.ACCESS1-3.historical.mon.seaIce.OImon.r1i1p1":

            # Set up_to_date to be False as default will be overwritten to true if found to be true
            ds.up_to_date = False
            ds.save()

            # Open and read cached JSON file
            esgf_dict, json_file = esgf_dict._generate_local_logdir(DATASET_LATEST_CACHE, ds, esgf_dict)
            logfile = os.path.join(DATASET_LATEST_DIR, os.path.basename(json_file).replace(".json", ".dataset.log"))
            json_resp = read_datafile_json_cache(ds, json_file, logfile)

            # versions is a dictionary where the key is the data_node and value is the published version
            versions = {}
            versions = get_all_versions(json_resp, versions, logfile )

            if ceda_data_node not in versions.keys():
                url = esgf_dict.format_is_latest_dataset_url()
                log_message(ds, logfile, "LATEST.001 [ERROR] :: Dataset is missing from CEDA archive :: JSON {}".format(url))
                print "Dataset is missing from CEDA archive {}".format(ds.esgf_drs)
                print url
            else:
                valid_lastest_version, latest_version = get_lastest_version(ds, versions, logfile)

                if valid_lastest_version:
                    ceda_version_is_latest = check_dataset_version(ds, versions, latest_version, ceda_data_node, logfile)
                    if ceda_version_is_latest:
                        log_message(ds, logfile, "LATEST.001 [PASS] :: CEDA version is up to date {}".format(ds.esgf_drs),
                                    set_uptodate=True)
                        # print "LATEST.001 [PASS] :: CEDA version is up to date {}".format(ds.esgf_drs)

                    else:
                        log_message(ds, logfile, "LATEST.001 [FAIL] :: CEDA version is NOT up to date {}".format(ds.esgf_drs))
                        print "LATEST.001 [FAIL] :: CEDA VERSION NOT UP TO DATE {}".format(ds.esgf_drs)
                        url = esgf_dict.format_is_latest_dataset_url()
                        print url

                else:
                    log_message(ds, logfile, "LATEST.001 [FAIL] :: No valid latest version found for {}".format(ds.esgf_drs))
                    print "LATEST.001 [FAIL] :: No valid latest version found for {}".format(ds.esgf_drs)
                    url = esgf_dict.format_is_latest_dataset_url()
                    print url


def datafile_latest_check(datasets, esgf_dict):
    """
        dataset_latest_check

    Driver to call functions to test whether the datafile version at CEDA is the latest.

    :param datasets: A Django QuerySet of Dataset objects
    :param esgf_dict: An esgf_dict object
    """

    ceda_data_node = "esgf-data1.ceda.ac.uk"
    version_qc = False
    ceda_cksum_is_latest = False
    for ds in datasets:

        dfs = ds.datafile_set.all()

        for df in dfs:

            #  print df.archive_path
            df.up_to_date = False
            df.save()

            esgf_dict, json_file = esgf_dict._generate_local_logdir(DATAFILE_LATEST_CACHE, ds, esgf_dict, subdir="exper",
                                                                     ncfile=df.ncfile)
            logfile = os.path.join(DATAFILE_LATEST_DIR, os.path.basename(json_file).replace(".json", ".datafile.log"))
            # print "json_file {}".format(json_file)

            # Open and read cached JSON file
            json_resp = read_datafile_json_cache(df, json_file, logfile)

            # versions is a dictionary where the key is the datanode and value is the published version
            checksums = {}
            checksums = get_all_checksums(json_resp, checksums, logfile)

            update_db_checksums(df, checksums, ceda_data_node)

            if ceda_data_node in checksums.keys():
                valid_latest_datafile, latest_checksum = get_latest_checksum(df, checksums, logfile)

                if valid_latest_datafile:
                    if isinstance(latest_checksum, dict):
                        ceda_cksum_is_latest = check_datafile_version_and_checksum(df, checksums, latest_checksum, ceda_data_node, logfile)
                    else:
                        log_message(df, logfile, "LATEST.000 [ERROR] :: Latest checksum is not a dictionary")
                else:
                    log_message(df, logfile, "LATEST.009 [ERROR] :: No latest datafile found")
            else:
                log_message(df, logfile, "LATEST.001 [ERROR] :: Datafile is missing from CEDA archive")
                ceda_cksum_is_latest = False

            if ceda_cksum_is_latest:
                log_message(df, logfile, "LATEST.000 [PASS] :: CEDA datafile is up to date CEDA checksum :: {} "
                                         "LATEST checksum {} LATEST source {}".format(
                                         checksums[ceda_data_node]['cksum'], latest_checksum['cksum'], latest_checksum['node']),
                            set_uptodate=True)

                # print "SUCCESS a valid datafile was found for {}".format(df.ncfile)
            else:
                url = utils._generate_datafile_url(df.ncfile)
                error_message = "LATEST.009 [FAIL] :: CEDA datafile is not latest version :: CEDA checksum is {} :: " \
                                "LATEST checksum is {} :: LATEST source is {} :: JSON QUERY :: {}".format(checksums[ceda_data_node]['cksum'],
                                latest_checksum['cksum'], latest_checksum['node'], url)

                log_message(df, logfile, error_message)

                print "FAIL a valid datafile was not found or is missing for {}".format(df.archive_path)
                print error_message


