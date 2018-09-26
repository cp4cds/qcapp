#!/usr/bin/env python

from setup_django import *
from settings import *
from utils import *
import os
import sys
import json
import commands


def add_to_database(df_data):

    # Make Data requester table
    dRequester, _ = DataRequester.objects.get_or_create(requested_by="CP4CDS")

    # Extract all information from df

    proj, product, institution, model, experiment, frequency, realm, table, ensemble, version, file, ext = \
    df_data['id'].split('|')[0].split('.')
    ncfile = '.'.join([file, ext])
    datanode = df_data['id'].split('|')[1]

    # Make Specification table and link to requester
    dSpec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=table, frequency=frequency)
    dSpec.datarequesters.add(dRequester)
    dSpec.save()

    drs = '.'.join(['proj', product, institution, model, experiment, frequency, realm, table, ensemble])
    dSet, _ = Dataset.objects.get_or_create(project="CMIP5",
                                            product=product,
                                            institute=institution,
                                            model=model,
                                            experiment=experiment,
                                            frequency=frequency,
                                            realm=realm,
                                            cmor_table=table,
                                            ensemble=ensemble,
                                            variable=variable,
                                            version=version,
                                            esgf_drs=drs,
                                            esgf_node=datanode
                                            )

    archive_filepath = os.path.join(ARCHIVE_ROOT, proj, product, institution, model, experiment, frequency, realm,
                                    table, ensemble, version, variable, ncfile)
    if not os.path.exists(archive_filepath):
        print("ERROR file not in archive....")

    gws_path = os.path.join(GWS_BASEDIR, product, institution, model, experiment, frequency, realm,
                                    table, ensemble, variable, 'latest', ncfile)
    if not os.path.exists(gws_path):
        print("ERROR file not in gws....")


    start_time, end_time = get_start_end_times(frequency, archive_filepath)
    md5_checksum = commands.getoutput('md5sum ' + archive_filepath).split(' ')[0]
    isTimeseries = is_timeseries(archive_filepath)

    if df_data["checksum_type"][0].strip() == "SHA256":
        sha256_checksum = df_data["checksum"][0].strip()
    else:
        sha256_checksum = commands.getoutput('sha256sum ' + archive_filepath).split(' ')[0]

    dFile, _ = DataFile.objects.get_or_create(dataset=dSet,
                                              archive_path=archive_filepath,
                                              gws_path=gws_path,
                                              ncfile=os.path.basename(archive_filepath),
                                              size=df_data["size"],
                                              sha256_checksum=sha256_checksum,
                                              md5_checksum=md5_checksum,
                                              tracking_id=df_data["tracking_id"][0].strip(),
                                              download_url=df_data["url"][0].strip(),
                                              variable=variable,
                                              variable_long_name=df_data["variable_long_name"][0].strip(),
                                              cf_standard_name=df_data["cf_standard_name"][0].strip(),
                                              variable_units=df_data["variable_units"][0].strip(),
                                              start_time=start_time,
                                              end_time=end_time,
                                              published=True,
                                              timeseries=isTimeseries
                                              )



def check_in_database(filename):

    """
    For a given file, this checks that there is not already a database entry

    :param ifile:
    :return:
    """
    df = DataFile.objects.filter(ncfile=filename)
    if len(df) == 1:
        return True
    else:
        model = filename.split('_')[2]
        # excluded models shouldn't be in database
        if 'MIROC' in model or 'MRI' in model:
            return True
        else:
            return False

def parse_json(variable, frequency, table, experiment):

    json_logdir, json_file = define_local_json_cache_names(variable, frequency, table, experiment)
    df_info = read_json(json_file)
    for df in df_info[:1]:

        in_db = check_in_database(df['title'])
        if not in_db:
            print "NOT in db {}".format(df['title'])
            add_to_database(df)


def db_make(variable, frequency, table):

    for experiment in ALLEXPTS:
        datafile_info = parse_json(variable, frequency, table, experiment)


if __name__ == "__main__":

    variable = sys.argv[1]
    frequency = sys.argv[2]
    table = sys.argv[3]

    db_make(variable, frequency, table)
