
import os
import datetime
import re
import glob
import json
import commands
import requests
import settings
import utils
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()



CEDA_DATA_NODE = "esgf-data1.ceda.ac.uk"




class EsgfDict(dict):

    def _format_gen_url(self, template, **kwargs):
        return template.format(**kwargs)

    def _generate_latest_dfpath(self, basedir, edict):

        json_dir = os.path.join(basedir, edict['variable'], edict['table'], edict['experiment'],
                                edict['ensemble'], edict['version'])
        json_file = edict['ncfile'].replace('.nc', '__latest_datafile.json')
        json_path = os.path.join(json_dir, json_file)

        return json_path


    def _generate_jsonfile_path(self, ds_obj, basedir, edict, dtype="dataset", ncfile=None, rw='r'):
        """

            _generate_local_logdir

        Uses a base directory with a database object (Dataset or DataFile) to build a file path.
        If "write mode" is selected the file path directories are constructed
        If the a sub-dircetory is selected an "experiment" sub-directory is included, for DataFile
        If an ncfile is specifed this is included as the path.

        :param basedir: A base directory
        :param ds_obj: A Django database object; Dataset
        :param edict: A esgf_dict object
        :param dtype: Set type to be dataset, or datafile, log at the experiment level for datafile
        :param rw: Read or Write mode
        :param ncfile: If DataFile object use the ncfile as the filename

        :return: Tuple of ([esgf_dict], [JSON-logfile])
        """

        if dtype not in ["dataset", "datafile"]:
            raise Exception("No Database object type specified")

        if dtype == "datafile":
            if ncfile == None:
                raise Exception("A netcdf file must be specifed for datafile type")

        if dtype == "dataset": logdir = basedir
        if dtype == "datafile": logdir = os.path.join(basedir, edict["experiment"])

        edict["institute"] = ds_obj.institute
        edict["model"] = ds_obj.model
        edict["realm"] = ds_obj.realm
        edict["ensemble"] = ds_obj.ensemble


        if dtype == "datafile":
            logfile = "{}.{}.json".format(ds_obj.project, ncfile.strip('.nc'))
        if dtype == "dataset":
            logfile = "{}.{}.json".format(ds_obj.esgf_drs, edict["variable"])

        json_file = os.path.join(logdir, logfile)

        if rw == 'w':
            if not os.path.isdir(logdir):
                os.makedirs(logdir)

        return edict, json_file


    def esgf_query(self, url, logfile):

        """

            esgf_query

        Perform an ESGF query based on the input url and log to the JSON file specified.

        :param url: ESGF URL query to ReST API
        :param logfile: JSON logfile
        """

        resp = requests.get(url, verify=False)
        json = resp.json()
        with open(logfile, 'w+') as fw:
            jsn.dump(json, fw)



    def format_is_latest_datafile_url(self):

        """

            format_is_latest_datafile_url

        Construct an is latest DataFile REST API URL query to ESGF

        :return: A latest, distributed, datafile, url query
        """

        template="https://{node}/esg-search/search?type=File&project={project}&time_frequency={frequency}&" \
                 "title={ncfile}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    frequency=self['frequency'],
                                    ncfile=self['ncfile'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


    def format_is_latest_dataset_url(self):

        """

            format_is_latest_dataset_url

        Construct an is latest Dataset REST API URL query to ESGF

        :return: A latest, distributed, dataset, url query
        """

        template="https://{node}/esg-search/search?type=Dataset&project={project}&institute={institute}&model={model}&" \
                 "experiment={experiment}&time_frequency={frequency}&realm={realm}&cmor_table={table}&ensemble={ensemble}&" \
                 "variable={variable}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    institute=self['institute'],
                                    model=self['model'],
                                    experiment=self['experiment'],
                                    frequency=self['frequency'],
                                    realm=self['realm'],
                                    table=self['table'],
                                    ensemble=self['ensemble'],
                                    variable=self['variable'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


class ESGFError(Exception):

    def __init__(self, message, dbobj):

        super(ESGFError, self).__init__(message)

        self.dbobj = dbobj
        _log_message(dbobj, message)
        print(message)

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


def _generate_checksum_dict(node, version, cksum_type, cksum):
    """
        _generate_checksum_dict
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


def _read_json_cache_file(df, json_file):
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
        raise ESGFError("IS_LATEST.000 [FAIL] :: NO JSON LOG FILE :: "
                        "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

    return json_resp


def get_latest_version(dbobj, versions):
    """
        get_latest_version
    Using the versions obtained from "get_all_versions" this function loops through each version and where the
    version number is of the YYYYMMDD form using the "_convert_version" function compares all the published versions
    and obtains the latest.
    :param versions: A list of versions
    :return: version number [string]
    """

    if not versions:
        raise ESGFError("IS_LATEST.006 [FAIL] :: No known master versions :: "
                        "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), dbobj)

    dt_versions = []
    for version in versions:
        dt_versions.append(_convert_version(version))

    try:
        latest_version = max(dt_versions)
        if isinstance(latest_version, datetime.datetime): latest_version = latest_version.strftime("%Y%m%d")

    except TypeError:
        raise ESGFError("IS_LATEST.006 [FATAL] :: No known latest version as types do not match {} :: "
                        "ESGF query {}".format(versions, esgf_dict.format_is_latest_datafile_url()), dbobj)

    return latest_version



def get_all_checksums(df, json_resp):
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
        raise ESGFError("[FAIL] :: No Results for this query:: "
                        "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

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


def get_latest_checksum(df, cksums):
    """
        get_latest_checksum
    From a dictionary object of checksums, determines if there is a valid latest checksum and returns it,
    else an error is recorded.
    :param cksums: A dictionary object of the form:
                   cksums[data_node] = {'replica':, 'version':, 'cksum_type':, 'cksum':}
    :return: latest_checksum [dict] {'node':, 'version':, 'cksum_type':, 'cksum':}
    """
    master_checksums = []
    master_versions = []
    all_versions = []
    n_masters = 0

    for key, values in cksums.items():
        # If record is a master record
        if values["replica"] == False:
            master_checksums.append(_generate_checksum_dict(key, values['version'], values['cksum_type'], values['cksum']))
            master_versions.append(values['version'])
            n_masters +=1

    if n_masters == 1:
        master_checksum = master_checksums[0]
        return master_checksum

    else:
        if n_masters == 0:
            for key, values in cksums.items():
                all_versions.append(values['version'])
            master_versions = all_versions

        latest_version = get_latest_version(df, master_versions)
        for k, v in cksums.items():
            if v['version'] == latest_version:
                master_checksum = v
                return master_checksum

        raise ESGFError("IS_LATEST [FATAL] :: No master single master version, unable to determine latest checksum "
                        "{}".format(esgf_dict.format_is_latest_datafile_url()), df)

def compare_ceda_with_latest(db_obj, ceda, latest, dbType):
    """
        compare_ceda_with_latest
    If the CEDA and latest versions are the same the function returns True, if they are different it returns False
    :param ceda_version: The CEDA dataset version for this datafile
    :param latest_version:
    :return: [Boolean]
    """
    if ceda == latest:
        return True

    else:
    # if ceda != latest:
        if dbType == "df":
            url = esgf_dict.format_is_latest_datafile_url()
            return False

        if dbType == "ds":
            url = esgf_dict.format_is_latest_dataset_url()

            error_message = "IS_LATEST [ERROR] :: CEDA is not the same as latest. CEDA is: {}, " \
                            "LATEST is: {} :: ESGF Query {}".format(ceda, latest, url)
            raise ESGFError(error_message, db_obj)



def get_ceda_version(df, cksums):
    """
        get_ceda_checksum
    Get and return the CEDA checksum from a dictionary of checksums
    :param cksums: cksums[data_node] = {'replica':, 'version':, 'cksum_type':, 'cksum':}
    :return: Tuple (checksum [string], error_message)
    """
    try:
        ceda_version = cksums[CEDA_DATA_NODE]['version']

    except:
        raise ESGFError("IS_LATEST [FATAL] :: unable to get a CEDA version:: "
                        "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

    return ceda_version


def get_ceda_checksum(df, cksums, checksum_type="SHA256"):
    """
        get_ceda_checksum
    Get and return the CEDA checksum from a dictionary of checksums
    :param cksums: cksums[data_node] = {'replica':, 'version':, 'cksum_type':, 'cksum':}
    :return: Tuple (checksum [string], error_message)
    """
    try:
        if checksum_type == "SHA256":
            ceda_cksum = cksums[CEDA_DATA_NODE]['cksum']
        else:
            ceda_cksum = df.md5_checksum

    except:
        raise ESGFError("IS_LATEST [FATAL] :: unable to get a CEDA checksum:: "
                        "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

    return ceda_cksum

#
# def check_datafiles_are_latest(datasets, esgf_dict):
#     """
#         dataset_latest_check
#     Runs test to check whether CP4CDS datafile is the most recent version on ESGF.
#     :param datasets: A Django QuerySet of Dataset objects
#     :param esgf_dict: An esgf_dict object
#     """
#
#     for ds in datasets:
#         dfs = ds.datafile_set.all()
#         check_datafile_is_latest(ds, dfs, esgf_dict)
#

def check_datafile_is_latest(df):

    frequency = df.dataset.frequency
    table = df.dataset.cmor_table
    variable = df.variable

    global esgf_dict
    esgf_dict = EsgfDict([
        ("node", "esgf-index1.ceda.ac.uk"),
        ("project", "CMIP5"),
        ("frequency", frequency),
        ("table", table),
        ("variable", variable),
        ("distrib", "true"),
        ("latest", "true"),
    ])

    try:

        df.up_to_date = False
        df.save()

        esgf_dict['ncfile'] = df.ncfile
        esgf_dict['ensemble'] = df.dataset.ensemble
        esgf_dict['version'] = df.dataset.version
        esgf_dict['experiment'] = df.dataset.experiment

        json_file = esgf_dict._generate_latest_dfpath(QCLOGS, esgf_dict)
        # # Update the esgf_dict and get the cached json file (a distributed is latest ESGF query)
        # esgf_dict, json_file = esgf_dict._generate_jsonfile_path(ds, DATAFILE_LATEST_CACHE, esgf_dict,
        #                                                          dtype="datafile", ncfile=df.ncfile)

        # Open and read cached JSON file
        json_resp = _read_json_cache_file(df, json_file)

        # checksums dictionary: {node: {'replica': T/F, 'version':, 'cksum_type': SHA256/MD5, 'cksum':}}
        checksums = get_all_checksums(df, json_resp)

        if CEDA_DATA_NODE not in checksums.keys():
            raise ESGFError("IS_LATEST [FATAL] :: File has no CEDA published record :: "
                            "ESGF query {}".format(esgf_dict.format_is_latest_datafile_url()), df)

        # Check whether the CEDA published file is the most recent by checksum comparison
        latest = get_latest_checksum(df, checksums)

        if latest['cksum_type'].upper() == "MD5":
            cksumType = "MD5"
        else:
            cksumType = "SHA256"

        ceda_checksum = get_ceda_checksum(df, checksums, checksum_type=cksumType)
        ceda_cksum_is_latest = compare_ceda_with_latest(df, ceda_checksum, latest['cksum'], dbType='df')

        ceda_version = get_ceda_version(df, checksums)
        ceda_version_is_latest = compare_ceda_with_latest(df, ceda_version, latest['version'], dbType='df')

        if ceda_cksum_is_latest and ceda_version_is_latest:
            success_message = "IS_LATEST [PASS] :: The CEDA datafile checksum {} is the same as the " \
                              "ESGF recorded latest {} :: ESGF query {}".format(
                               ceda_checksum, latest['cksum'], esgf_dict.format_is_latest_datafile_url())
            # print(success_message)
            _log_message(df, success_message, set_uptodate=True)

        elif ceda_cksum_is_latest and not ceda_version_is_latest:
            raise ESGFError("IS_LATEST [VERSION ERROR] :: The CEDA datafile checksum {} is the same as the "
                            "ESGF recorded latest {} :: CEDA version {} is not latest {} and should be updated."
                            " :: ESGF query {}".format(ceda_checksum, latest['cksum'], ceda_version,
                                            latest['version'], esgf_dict.format_is_latest_datafile_url()), df)
        else:
            raise ESGFError("IS_LATEST [CHECKSUM ERROR] :: Checksum do not match CEDA {} latest {} :: CEDA version {}, "
                            "latest version {} :: ESGF query {}".format(ceda_checksum, latest['cksum'],
                                                                        ceda_version, latest['version'],
                                                                        esgf_dict.format_is_latest_datafile_url()),
                                                                        df)

    except ESGFError:
        pass
