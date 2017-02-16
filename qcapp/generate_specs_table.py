import django

django.setup()

from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg
import collections, os, timeit, datetime
import requests, itertools
# IMPORT QC STUFF (only works in venv27)
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
URL_FILE_ENSEMBLE_FACETS = 'https://%(node)s/esg-search/search?type=File&project=%(project)s&variable=%(variable)s&' \
                           'cmor_table=%(table)s&time_frequency=%(frequency)s&model=%(model)s&experiment=%(experiment)s&' \
                           'ensemble=%(ensemble)s&latest=%(latest)s&distrib=%(distrib)s&facets=ensemble&format=application%%2Fsolr%%2Bjson'

def get_spec_info(d_spec, project, variable, table, frequency, expts, node, distrib, latest):
    """
    Uses ESGF RESTful API to query ESGF and writes information for a given variable, table and frequency
    to the dataset and datafile models.

    :param variable: from data specification
    :param table:    from data specification
    :param frequency:from data specification
    :return: output to cache file
    """

    # CHECK IF DS EXISTS?



    for experiment in expts:
        url = URL_DS_MODEL_FACETS % vars()
        resp = requests.get(url)
        json = resp.json()
        models = json["facet_counts"]["facet_fields"]["model"]
        models = dict(itertools.izip_longest(*[iter(models)] * 2, fillvalue=""))
        check_valid_model_names(models)  # Translates some names from ESGF to archive friendly names

        for model in models.keys():

                url = URL_DS_ENSEMBLE_FACETS % vars()
                resp = requests.get(url)
                json = resp.json()
                ensembles = json["facet_counts"]["facet_fields"]["ensemble"]
                ensembles = dict(itertools.izip_longest(*[iter(ensembles)] * 2, fillvalue=""))

                for ensemble in ensembles.keys():

                    # EXTRACT ALL INFORMATION REQUIRED FOR A DATASET RECORD
                    json_resp = json["response"]["docs"][0]
                    product = json_resp["product"][0]
                    institute = json_resp["institute"][0]
                    realm = json_resp["realm"][0]
                    version = json_resp["version"]

                    # MAKE A DATASET RECORD
                    ds = create_dataset_record(project, product, institute, model, experiment, frequency,
                                               realm, table, ensemble, version, variable)
                    link_dataset_to_specification(ds, d_spec)

                    # GET ALL FILES INFORMATION RELATED TO DATASET
                    url = URL_FILE_ENSEMBLE_FACETS % vars()
                    resp = requests.get(url)
                    json = resp.json()

                    datafiles = json["response"]["docs"]
                    for df in range(len(datafiles)):
                        fname = datafiles[df]["master_id"]
                        filepath = parse_filename(fname)
                        ceda_filepath = create_ceda_filepath(filepath, version, variable)
                        print filepath
                        start_time, end_time = get_start_end_times(frequency, filepath)
                        size = datafiles[df]["size"]
                        try:
                            checksum = datafiles[df]["checksum"][0]
                        except AttributeError:
                            checksum = ''
                        tracking_id = datafiles[df]["tracking_id"][0]
                        download_url = datafiles[df]["url"][0]
                        variable_long_name = datafiles[df]["variable_long_name"][0]
                        cf_standard_name = datafiles[df]["cf_standard_name"][0]
                        variable_units = datafiles[df]["variable_units"][0]
                        data_node = datafiles[df]["data_node"]


                        # Create a Datafile record for each file
                        create_datafile_record(ds, filepath, ceda_filepath, size, checksum, download_url, data_node, tracking_id,
                                               variable, cf_standard_name, variable_long_name, variable_units,
                                               start_time, end_time)

                    add_long_name_to_dspec(d_spec, variable_long_name)
                    ds.exists = True
                    ds.save()

#def perform_qc():


    # RUN CF CHECKER
    # run_cf_checker(ceda_filepath)

    # RUN CEDA-CC
    # run_ceda_cc(ceda_filepath)
    # parse_ceda_cc()



def get_no_models_per_expt(d_spec, expts):

    for experiment in expts:

        models_object = Dataset.objects.filter(variable=d_spec.variable, cmor_table=d_spec.cmor_table,
                                               frequency=d_spec.frequency, experiment=experiment)
        models_by_experiment = {}
        mlist = set([])
        for m in models_object.all():
             mlist.add(m.model)
             models_by_experiment[experiment] = mlist

    in_all = None
    for key, items in models_by_experiment.items():
        if in_all is None:
            in_all = items
        else:
            in_all.intersection_update(items)

    d_spec.number_of_models = len(in_all)
    print len(in_all)
    d_spec.save()


def check_valid_model_names(models):

   for model in models:
        if model == "CESM1(CAM5)":
            model = "CESM1-CAM5"
            #print "replaced: ", model
        if model == "BCC-CSM1.1(m)":
            model = "bccDa-csm1-1-m"
            #print "replaced: ", model
        if model == "ACCESS1.0":
            model = "ACCESS1-0"
            #print "replaced: ", model

def create_ceda_filepath(path, version, variable):
    path = path.split('/')
    vid = 'v'+version
    path.insert(-1, vid)
    path.insert(-1, variable)
    path = os.path.join('', *path)
    path = '/badc/cmip5/data/' + path

    return path

def run_cf_checker(qcfile):
    cf = CFChecker(cfStandardNamesXML=STANDARDNAME, cfAreaTypesXML=AREATYPES, version=newest_version)
    resp = cf.checker(qcfile)

def run_ceda_cc(qcfile):
    # run the ceda-cc - generate the qcBatch log.
    # write list to a file for use by ceda-cc
    odir = '/usr/local/cp4cds-app/ceda-cc-output'

    m = c4.main(args=['-p', 'CMIP5', '-f', qcfile, '--ld', odir])
    # need to now parse output in the odir

def parse_ceda_cc():
    """Parse the qcBatch log for fails

    Makes a dictotionary with arrivals file path and status
    Run the bactch log parser to work out where the file is going.
      - good files are marked as good
      - Minor fail - just one fail - Work out manually / contact data provider /manually pass
      - Fail - more than filure reported by ceda-cc - Work out manually / contact data provider
      - Exceptions - where ceda-cc trows an exception - Work out manually
    """
    logfiles = glob.glob("logs_02/**")
    if len(logfiles) != 1:
        log("unexpected number of ceda-cc output files")
        return "ERROR"

    logfile = logfiles[0]

    for line in open(logfile):
        match = re.search(r'Done -- error count (\d+)', line)
        if match:
            errors = int(match.group(1))
        match = re.search(r'ERROR Exception has occured', line)
        if match:
            return "exception"
        match = re.search(r'INFO Done -- testing aborted because of severity of errors', line)
        if match:
            return "abort"
        match = re.search(r'ERROR C4\.\d{3}\.\d{3}: (.*)', line)
        if match:
            lasterror = match.groups()[0]

    if errors == 0:
        return "pass"
    if errors == 1:
        return lasterror
    if errors > 1:
        return "fail"

def parse_filename(fname):
    fname = fname.replace('.','/')
    fname = fname.replace('/nc','.nc')
    fname = '/'+fname
    return  fname

def get_start_end_times(frequency, fname):

    if fname.endswith('.nc'):
        if frequency == 'mon':
            start_time = datetime.date(int(fname[-16:-12]), int(fname[-12:-10]), 01)
            end_mon = fname[-5:-3]
            if (end_mon == '01') or (end_mon == '03') or (end_mon == '05') or (end_mon == '07') \
                    or (end_mon == '08') or (end_mon == '10') or (end_mon == '12'):
                end_day = 31
            elif (end_mon == '04') or (end_mon == '06') or (end_mon == '09') or (end_mon == '11'):
                end_day = 30
            else:
                end_day = 28
        end_time = datetime.date(int(fname[-9:-5]), int(fname[-5:-3]), end_day)

        if frequency == 'day':
            start_time = datetime.date(int(fname[-20:-16]), int(fname[-16:-14]), int(fname[-14:-12]))
            end_time = datetime.date(int(fname[-11:-7]), int(fname[-7:-5]), int(fname[-5:-3]))
    else:
        start_time = datetime.date(1900, 1, 1)
        end_time = datetime.date(1999, 12, 31)

    return start_time, end_time

def create_dataset_record(project, product, institute, model, experiment, frequency,
                          realm, table, ensemble, version, variable):

    ds, result = Dataset.objects.get_or_create(project=project,
                                               product=product,
                                               institute=institute,
                                               model=model,
                                               experiment=experiment,
                                               frequency=frequency,
                                               realm=realm,
                                               cmor_table=table,
                                               ensemble=ensemble,
                                               variable=variable,
                                               version=version,
                                               start_time=datetime.date(2000, 01, 01),
                                               end_time=datetime.date(2000, 01, 01)
                                               )
    ds.save()
    return ds



def create_datafile_record(ds, filepath, ceda_filepath, size, checksum, download_url, index_node, tracking_id,
                           variable, variable_cf_name, variable_long_name, variable_units, start_time, end_time):

    newfile, result = DataFile.objects.get_or_create(dataset=ds,
                                                     filepath=filepath,
                                                     archive_path=ceda_filepath,
                                                     size=size,
                                                     checksum=checksum,
                                                     download_url=download_url,
                                                     tracking_id=tracking_id,
                                                     data_node=index_node,
                                                     variable=variable,
                                                     cf_standard_name=variable_cf_name,
                                                     variable_long_name=variable_long_name,
                                                     variable_units=variable_units,
                                                     start_time=start_time,
                                                     end_time=end_time
                                                     )

    newfile.save()



def create_specs_record(variable, cmor_table, frequency):
    """
    Create a data specification record
    """
    specs, result = DataSpecification.objects.get_or_create(variable=variable,
                                                            cmor_table=cmor_table,
                                                            frequency=frequency
                                                            )
    specs.save()
    return specs

def create_requester_record(requester):
    """
    Create a Requester record
    """
    req, result = DataRequester.objects.get_or_create(requested_by=requester)
    req.save()
    return req

def link_requester_to_specification(d_spec, d_requester):
    d_spec.datarequesters.add(d_requester)
    d_spec.save()

def link_dataset_to_specification(ds, d_spec):
    ds.data_spec.add(d_spec)
    ds.save()

def add_long_name_to_dspec(d_spec, variable_long_name):
    d_spec.variable_long_name = variable_long_name
    d_spec.save()

def read_project_data_specs(file):
    reader = open(file, 'r')
    data = reader.readlines()
    reader.close()

    return data



if __name__ == '__main__':

    # These constraints will in time be loaded in via csv for multiple projects.
    project = 'CMIP5'
    # cp4cds-app1-test can't see ceda node?? for testing for now using dkrz
    node = 'esgf-data.dkrz.de'
    #node = "esgf-index1.ceda.ac.uk"
    expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
    file = '/usr/local/cp4cds-app/project-specs/cp4cds-dmp_data_request.csv'
    data = read_project_data_specs(file)
    lineno = 0
    for line in data:
        if lineno == 0:
            requester = line.split(',')[0].strip()
            d_requester = create_requester_record(requester)

        if lineno > 1:
            variable = line.split(',')[0].strip()
            table = line.split(',')[1].strip()
            frequency = line.split(',')[2].strip()
            # passing in d_spec to add in variable long name retrieved from ESGF
            #variable_long_name = line.split(',')[3].strip()
            #number_experiments = line.split(',')[4].strip()
            #number_of_models = line.split(',')[5].strip()
            #volume_of_data = line.split(',')[6].strip()

            # Create spec record and link to requester
            d_spec = create_specs_record(variable, table, frequency)
            link_requester_to_specification(d_spec, d_requester)

            get_spec_info(d_spec, project, variable, table, frequency, expts, node, distrib=False, latest=True)
            get_no_models_per_expt(d_spec, expts)
        lineno += 1