import django
django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

import collections, os, timeit, datetime, time, re
import requests, itertools

from ceda_cc import c4
from cfchecker.cfchecks import CFVersion, CFChecker, STANDARDNAME, AREATYPES, newest_version

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings()

# URL TEMPLATES
URL_DS_MODEL_FACETS = 'https://%(node)s/esg-search/search?type=Dataset&project=%(project)s&variable=%(variable)s' \
                   '&cmor_table=%(table)s&time_frequency=%(frequency)s&experiment=%(experiment)s&latest=%(latest)s&distrib=%(distrib)s&' \
                   'facets=model&format=application%%2Fsolr%%2Bjson'
URL_DS_ENSEMBLE_FACETS = 'https://%(node)s/esg-search/search?type=Dataset&project=%(project)s&variable=%(variable)s' \
                      '&cmor_table=%(table)s&time_frequency=%(frequency)s&model=%(model)s&experiment=%(experiment)s&' \
                      'latest=%(latest)s&distrib=%(distrib)s&facets=ensemble&format=application%%2Fsolr%%2Bjson'
URL_FILE_INFO = 'https://%(node)s/esg-search/search?type=File&project=%(project)s&variable=%(variable)s&' \
                'cmor_table=%(table)s&time_frequency=%(frequency)s&model=%(model)s&experiment=%(experiment)s&' \
                'ensemble=%(ensemble)s&latest=%(latest)s&distrib=%(distrib)s&format=application%%2Fsolr%%2Bjson&limit=10000'


def get_spec_info(d_spec, project, variable, table, frequency, expts, node, distrib, latest):
    """
    Uses ESGF RESTful API to query ESGF and writes information for a given variable, table and frequency
    to the dataset and datafile models.

    :param variable: from data specification
    :param table:    from data specification
    :param frequency:from data specification
    :return: output to cache file
    """
    for experiment in expts:

        models, json = esgf_ds_search(URL_DS_MODEL_FACETS, 'model', project, variable, table, frequency, experiment,
                                '', node, distrib, latest)
        check_valid_model_names(models)  # Translates some names from ESGF to archive friendly names

        for model in models.keys():

            ensembles, json = esgf_ds_search(URL_DS_ENSEMBLE_FACETS, 'ensemble', project, variable, table, frequency, experiment,
                                              model, node, distrib, latest)

            for ensemble in ensembles.keys():

                # EXTRACT ALL INFORMATION REQUIRED FOR A DATASET RECORD
                product, institute, realm, version, esgf_ds_id, esgf_node \
                    = extract_ds_info(json["response"]["docs"][0])
                # MAKE A DATASET RECORD
                ds, _ = Dataset.objects.get_or_create(project=project, product=product, institute=institute,
                                                      model=model, experiment=experiment, frequency=frequency,
                                                      realm=realm, cmor_table=table, ensemble=ensemble,
                                                      variable=variable, version=version, esgf_ds_id=esgf_ds_id,
                                                      esgf_node=esgf_node)

                #LINK DATASET TO SPEC
                ds.data_spec.add(d_spec)
                ds.save()

                # GET ALL FILES INFORMATION RELATED TO DATASET
                filepath, ceda_filepath, start_time, end_time, size, checksum, tracking_id, download_url, \
                variable_long_name, cf_standard_name, variable_units = \
                    get_all_datafile_info(URL_FILE_INFO, ds, project, variable, table, frequency,
                                          experiment, model, ensemble, version, node, distrib, latest)

                # ADD VARIABLE LONG NAME TO SPECIFICATION
                d_spec.variable_long_name = variable_long_name
                d_spec.save()

                ds.exists = True
                ds.save()
    d_spec.esgf_data_collected = True
    d_spec.save()


def esgf_ds_search(search_template, facet_check, project, variable, table, frequency, experiment, model, node, distrib, latest):
    """
    Perform an esgf dataset search using the specified template

    :return: dictionary of facets
    """
    url = search_template % vars()
    resp = requests.get(url, verify=False)
    json = resp.json()
    result = json["facet_counts"]["facet_fields"][facet_check]
    result = dict(itertools.izip_longest(*[iter(result)] * 2, fillvalue=""))

    return result, json


def extract_ds_info(json_resp):
    """
    Parse json data
    :param json_resp:
    :return:
    """
    product = json_resp["product"][0].strip()
    institute = json_resp["institute"][0].strip()
    realm = json_resp["realm"][0].strip()
    version = json_resp["version"].strip()
    esgf_ds_id = json_resp["drs_id"][0].strip()
    esgf_node = json_resp["data_node"].strip()

    return product, institute, realm, version, esgf_ds_id, esgf_node


def get_all_datafile_info(url_template, ds, project, variable, table, frequency, experiment, model, ensemble,
                          version, node, distrib, latest):

    """
    Get all datafile information for a given dataset

    :return:
    """
    url = url_template % vars()
    resp = resp = requests.get(url, verify=False)
    json = resp.json()

    datafiles = json["response"]["docs"]
    for datafile in range(len(datafiles)):
        df = datafiles[datafile]
        fname = df["master_id"]
        filepath = parse_filename(fname)
        ceda_filepath = create_ceda_filepath(filepath, version, variable)
        start_time, end_time = get_start_end_times(frequency, filepath)
        size = df["size"]

        try:
            checksum = df["checksum"][0].strip()
        except AttributeError:
            checksum = ''

        tracking_id = df["tracking_id"][0].strip()
        download_url = df["url"][0].strip()
        variable_long_name = df["variable_long_name"][0].strip()
        cf_standard_name = df["cf_standard_name"][0].strip()
        variable_units = df["variable_units"][0].strip()

        # Create a Datafile record for each file
        newfile, _ = DataFile.objects.get_or_create(dataset=ds, filepath=filepath, archive_path=ceda_filepath,
                                                    size=size, checksum=checksum, download_url=download_url,
                                                    tracking_id=tracking_id, variable=variable,
                                                    cf_standard_name=variable_cf_name,
                                                    variable_long_name=variable_long_name,
                                                    variable_units=variable_units, start_time=start_time,
                                                    end_time=end_time)

    return filepath, ceda_filepath, start_time, end_time, size, checksum, tracking_id, download_url, \
           variable_long_name, cf_standard_name, variable_units


def get_no_models_per_expt(d_spec, expts):
    """
    Calculate the number of models which have a given variable in a set of specfied experiments
    Calculate the associated data volume

    :return:
    """

    ############################################################################################################
    # GENERATE A LIST OF MODELS FOR A GROUP OF EXPERIMENTS WHERE A GIVEN VARIABLE EXISTS IN ALL THE EXPERIMENTS
    # RESULT IS STORED IN A DICTIONARY
    models_by_experiment = {}
    for experiment in expts:
        datasets = Dataset.objects.filter(variable=d_spec.variable, cmor_table=d_spec.cmor_table,
                                          frequency=d_spec.frequency, experiment=experiment)
        models = set([])
        for dataset in datasets.all():
            models.add(dataset.model)
        models_by_experiment[experiment] = list(models)

    valid_models = check_in_all_models(d_spec, models_by_experiment)
    print valid_models
    ############################################################################################################


    ############################################################################################################
    # CALCULATE DATA VOLUMES FOR ALL VALID MODELS
    d_spec.data_volume = 0.0
    volume = 0.0
    #Dataset.objects.all().prefetch_related('')
    for model in valid_models:
        for experiment in expts:
            ds = Dataset.objects.filter(variable=d_spec.variable, cmor_table=d_spec.cmor_table,
                                        frequency=d_spec.frequency, experiment=experiment, model=model)
            for d in ds.all():
                volume += d.datafile_set.all().aggregate(Sum('size')).values()[0]
    d_spec.data_volume = volume / (1024. ** 3)
    print d_spec.variable, d_spec.frequency, d_spec.data_volume, len(valid_models)
    d_spec.save()
    ############################################################################################################


def check_in_all_models(d_spec, models_per_experiment):
    """
    Perform an intersection check to determine
    what list of models all variables are in over a
    dictionary of experiments and with list of models
    """

    in_all = None
    num_models = 0
    for key, models in models_per_experiment.iteritems():
        if in_all is None:
            in_all = set(models)
            num_models = len(in_all)
        else:
            in_all.intersection_update(models)
            num_models = len(in_all)
    d_spec.number_of_models = num_models
    d_spec.save()

    if not in_all:
        return []
    else:
        return list(in_all)


def check_valid_model_names(models):
    """
    Modify invalid model names
    :return:
    """
    for model in models:
        if model == "CESM1(CAM5)":
            model = "CESM1-CAM5"
        if model == "CESM1(WACCM)":
            model = "CESM1-WACCM"
                #print "replaced: ", model
        if model == "BCC-CSM1.1(m)":
            model = "bccDa-csm1-1-m"
            #print "replaced: ", model
        if model == "ACCESS1.0":
            model = "ACCESS1-0"
            #print "replaced: ", model
        if model == "ACCESS1.3":
            model = "ACCESS1-3"
        if model == "BCC-CSM1.1":
            model = "bcc-csm1-1"
        if model == "INM-CM4":
            model = "inmcm4"


def parse_filename(fname):
    """
    Parse a file name to get a file path from a DRS
    :return: filepath
    """
    fname = fname.replace('.','/')
    fname = fname.replace('/nc','.nc')
    fname = '/'+fname
    return fname


def get_start_end_times(frequency, fname):
    """
    Get start and end times from the filename
    :return:
    """

    if fname.endswith('.nc'):
        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if end_mon == '02':
                end_day = 28
            elif end_mon in ['04', '06', '09', '11']:
                end_day = 30
            else:
                end_day = 31
            end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time


def create_ceda_filepath(path, version, variable):
    """
    Generate the archive location of a file from the esgf path, version and variable

    :return:
    """

    path = path.split('/')
    vid = 'v'+version
    path.insert(-1, vid)
    path.insert(-1, variable)
    path = os.path.join('', *path)
    path = '/badc/cmip5/data/' + path

    return path


def run_cf_checker(qcfile, d_file):
    """
    
    :param qcfile: single file with full CEDA archive path to be checked
    :param d_file: the DataFile object to link the CF results to
    
    :result The results of the CF-Checker are stored in the QCerror and QCchecks tables 
    
    Run the CF checker in in-line mode with auto-version of file detection for a single file.
    The following options are used in the CF-Checker:
        cfStandardNamesXML=STANDARDNAME, 
        cfAreaTypesXML=AREATYPES, 
        version=CFVersion(), 
        silent=True 
    
    
    """

    
    print ""
    print ""
    print "RUNNING CF CHECKER", qcfile
    print ""
    print ""

    cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, version=CFVersion(), silent=True)

    start_time = timeit.default_timer()
    resp = cf.checker(qcfile)
    time_taken = timeit.default_timer() - start_time
    print "Time taken for a single file CF check is ", time_taken

    error_msgs = []
    vars = resp.items()[0]
    for message in vars[1].values():
        if len(message['FATAL']) > 0:
            error_msgs.append('FATAL: ' + message['FATAL'][0])
        if len(message['ERROR']) > 0:
            error_msgs.append('ERROR: ' + message['ERROR'][0])
        if len(message['WARN']) > 0:
            error_msgs.append('WARNING: ' + message['WARN'][0])
        if len(message['INFO']) > 0:
            error_msgs.append('INFO: ' + message['INFO'][0])

    gll = resp.items()[1]
    if gll[1]['FATAL']:
        error_msgs.append('FATAL: ' + gll[1]['FATAL'][0])
    if gll[1]['ERROR']:
        error_msgs.append('ERROR: ' + gll[1]['ERROR'][0])
    if gll[1]['WARN']:
        error_msgs.append('WARN: ' + gll[1]['WARN'][0])
    if gll[1]['INFO']:
        error_msgs.append('INFO: ' + gll[1]['INFO'][0])

    # Record CF output to database tables
    qc_check_table = make_qc_check_table('CF', d_file)
    for err in error_msgs:
        qc_err, _ = QCerror.objects.get_or_create(qc_check=qc_check_table, qc_error=err)


def make_qc_check_table(qcCheck, df):
    qc_check_table = QCcheck(qc_check_type=qcCheck)
    qc_check_table.save()
    qc_check_table.file_qc.add(df)
    qc_check_table.save()
    return qc_check_table

def run_ceda_cc(file, d_file, odir):
    """
    Run CEDA-CC on a single file with the following options, generating a qcBatch log
        -p CMIP5
        -f file
        --log multi
        --ld 
        --cae
        --blfmode a
    
    Output is written to log file directly parsed and (TODO deleted)

    :result: CEDA-CC errors recorded in QCerror
    """

    # Run CEDA-CC
    cedacc_args = ['-p', 'CMIP5', '-f', file, '--log', 'multi', '--ld', odir, '--cae', '--blfmode', 'a']
    _ = c4.main(cedacc_args)

    # CEDA-CC filename
    ceda_cc_file = odir + '/' + file.split('/')[-1][:-3] + '__qclog_' + time.strftime("%Y%m%d") + '.txt'

    # Read in CEDA-CC output
    with open(ceda_cc_file, 'r') as reader:
        ceda_cc_out = reader.readlines()

    # Identify where CEDA-CC picks up a QC error
    cedacc_error = re.compile('.*FAILED::.*')
    error_msgs = []
    for line in ceda_cc_out:
        if cedacc_error.match(line.strip()):
            error_msgs.append(line)

    # Make a CEDA-CC qc_check table and qc_error tables for all CEDA-CC errors
    qc_check_table = make_qc_check_table('CEDA-CC', d_file)
    for err in error_msgs:
        qc_err, _ = QCerror.objects.get_or_create(qc_check=qc_check_table, qc_error=err)


def perform_qc():
    """
    Perform the quality control
    Generate CEDA-CC files and parse output
    Perform CF-checks

    :return:
    """

#    for dataset in Dataset.objects.all():
    dataset = Dataset.objects.first()
    dsid = dataset.esgf_ds_id
    odir = os.path.join('/usr/local/cp4cds-app/ceda-cc-log-files/', *dsid.split('.')[2:])
    if not odir:
        os.makedirs(odir)

    datafiles = dataset.datafile_set.all()
    for d_file in datafiles:
        qcfile = str(d_file.archive_path)
        if qcfile:
            # Run CEDA-CC, including parsing of output and recording of error output
            run_ceda_cc(qcfile, d_file, '/usr/local/cp4cds-app/ceda-cc-log-files/')

            # Run CF checker and record error output
            run_cf_checker(qcfile, d_file)



def generate_data_records(project, node, expts, file, distrib, latest):
    """
    Generate data records from csv input
    :return:
    """
    with open(file, 'r') as reader:
        data = reader.readlines()

    lineno = 0
    for line in data:
        if lineno == 0:
            requester = line.split(',')[0].strip()
            d_requester, _ = DataRequester.objects.get_or_create(requested_by=requester)

        if lineno > 1:
            variable = line.split(',')[0].strip()
            table = line.split(',')[1].strip()
            frequency = line.split(',')[2].strip()

            # Create spec record and link to requester
            if DataSpecification.objects.filter(variable=variable, cmor_table=table, frequency=frequency, esgf_data_collected=False):
                d_spec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=cmor_table, frequency=frequency)
                d_spec.datarequesters.add(d_requester)
                d_spec.save()
                get_spec_info(d_spec, project, variable, table, frequency, expts, node, distrib, latest)
        lineno += 1

if __name__ == '__main__':

    # These constraints will in time be loaded in via csv for multiple projects.
    project = 'CMIP5'
    node = "172.16.150.171"
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    distrib = False
    latest = True
    file = '/usr/local/cp4cds-app/project-specs/cp4cds-dmp_data_request.csv'
    generate_data_records(project, node, expts, file, distrib, latest)
