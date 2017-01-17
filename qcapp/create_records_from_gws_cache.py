import django

django.setup()

import datetime
import os
from qcapp.models import *
from django.db.models import Count, Max, Min, Sum, Avg

def temporal_aggregation(dataset):
    """
    Aggregate datafiles timestamps to update dataset file stamp
    :input:
    dataset: a dataset to aggregate time over

    :return:
    """
    datafiles = dataset.datafile_set.all()
    dataset.start_time = datafiles.aggregate(Min('start_time'))
    dataset.end_time = datafiles.aggregate(Max('end_time'))



def aggregate():
    """
    Aggregate datafiles timestamps to update dataset file stamp
    :return:
    """



def create():
    """
    Files have the format
    filename, dataset_id, version, download_url, variable, variable_cf_name, variable_long_name, variable_units, \
    experiment_family, product, filesize, forcings, checksum_type, checksum, tracking_id, index_node, replica
    """

    cache_dir = '/group_workspaces/jasmin/cp4cds1/data_availability/cache'
    for path, dirs, files in os.walk(cache_dir):
        for file in files:
            filename = os.path.join(path, file)
            reader = open(filename, 'r')
            for line in reader:
                line = line.split(';')
                filename = line[0].strip()
                dataset_id = line[1].strip()
                version = line[2].strip()
                download_url = line[3].strip()
                variable = line[4].strip()
                variable_cf_name = line[5].strip()
                variable_long_name = line[6].strip()
                variable_units = line[7].strip()
                experiment_family = line[8]
                experiment_family = experiment_family[3:-1].strip()
#                product = line[9].strip()
                filesize = line[10].strip()
                forcings = line[11]
                forcings = forcings[4:-2].strip()
                checksum_type = line[12].strip()
                checksum = line[13].strip()
                tracking_id = line[14].strip()
                index_node = line[15].strip()
                replica = line[16].strip()



                product, institute, model, experiment, frequency, realm, table, ensemble, \
                filen, start_time, end_time = filename_parse(filename)

                """
                print 'Product: ', product
                print ("filename: %s \n dataset_id: %s \n version: %s \n download url: %s \n variable %s \n "
                       "variable cf name %s \n variable long name: %s \n variable units: %s \n experiment family: %s \n "
                       "product: %s \n filesize: %s \n forcings: %s \n checksum type: %s \n checksum: %s \n"
                       "tracking id: %s \n index node: %s \n replica: %s, institute: %s \n model: %s \n "
                       "experiment: %s \n frequency: %s \n realm: %s \n table: %s \n ensemble: %s \n filen: %s \n "
                       "start time: %s \n end time: %s \n ") % \
                      (filename, dataset_id, version, download_url, variable, variable_cf_name, variable_long_name,
                       variable_units, experiment_family, product, filesize, forcings, checksum_type, checksum,
                       tracking_id, index_node, replica, institute, model, experiment, frequency, realm, table, ensemble,
                       filen, start_time, end_time)
                """

                create_records(product, institute, model, experiment, frequency, realm, table, ensemble, version,
                               experiment_family, forcings, filename, filesize, checksum, download_url, index_node,
                               variable, variable_cf_name, variable_long_name, variable_units, tracking_id,
                               start_time, end_time)
            reader.close()

def create_records(product, institute, model, experiment, frequency, realm, table, ensemble, version, experiment_family,
                   forcings, filename, filesize, checksum, download_url, index_node, variable, variable_cf_name,
                   variable_long_name, variable_units, tracking_id, start_time, end_time):
    """
    Create a Dataset and DataFile record
    """
    ds, result = Dataset.objects.get_or_create(project="cmip5",
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
                                               experiment_family=experiment_family,
                                               forcing=forcings,
                                               start_time=datetime.date(2012, 12, 12),
                                               end_time=datetime.date(2016, 12, 12)
                                               )

    ds.save()

    newfile, result = DataFile.objects.get_or_create(dataset=ds,
                                                     filename=filename,
                                                     size=filesize,
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

def filename_parse(filename):
    """
    Parse filename into facets

    :param filename: filename to parse
    :return: institute, model, experiment, frequency, realm, table, ensemble, filen,
             start_year, start_month, start_day, end_year, end_month, end_day
    """

    filename = filename.split('/')
    product = filename[5].strip()
    institute = filename[6].strip()
    model = filename[7].strip()
    experiment = filename[8].strip()
    frequency = filename[9].strip()
    realm = filename[10].strip()
    table = filename[11].strip()
    ensemble = filename[12].strip()
    version = filename[13].strip()
    variable = filename[14].strip()
    filen = filename[15].strip()
    filen = filen.strip('.nc')
    filen = filen.split('_')

    if model == "CESM1(CAM5)":
        model = "CESM1-CAM5"
        print "replaced: ", model
    if model == "BCC-CSM1.1(m)":
        model = "bccDa-csm1-1-m"
        print "replaced: ", model
    if model == "ACCESS1.0":
        model = "ACCESS1-0"
        print "replaced: ", model

    if frequency == 'mon':

        start_year = int(filen[-1][0:4])
        start_month = int(filen[-1][4:6])
        start_day = int(01)

        end_year = int(filen[-1][7:11])
        end_month = int(filen[-1][11:13])

        if (end_month == 2):
            end_day = int(28)

        if (end_month == 4) or (end_month == 6) or (end_month == 9) or (end_month == 11):
           end_day = int(30)

        if (end_month == 1) or (end_month == 3) or (end_month == 5) or (end_month == 7) or \
                (end_month == 8) or (end_month == 10) or (end_month == 12):
           end_day = int(31)

    if frequency == 'day':

        start_year = int(filen[-1][0:4])
        start_month = int(filen[-1][4:6])
        start_day = int(filen[-1][6:8])
        end_year = int(filen[-1][9:13])
        end_month = int(filen[-1][13:15])
        end_day = int(filen[-1][15:17])

    start_time = datetime.date(start_year, start_month, start_day)
    end_time = datetime.date(end_year, end_month, end_day)

    return product, institute, model, experiment, frequency, realm, table, ensemble, filen, \
           start_time, end_time

"""
print "Create a Dataset..."
ds1 = Dataset(project="cmip5", product="output1", institute="MOHC",
              model="HadGEM2-ES", experiment="rcp45", frequency="mon",
              realm="atmos", cmor_table="Amon", ensemble="r1i1p1",
              version="v20140101", experiment_family="RCPs",
              forcing="ocean-comedy", start_time=datetime.datetime(2000, 1, 1),
              end_time=datetime.datetime(2100, 1, 1))
ds1.save()

print "Query record count..."
print Dataset.objects.count()

rec = Dataset.objects.filter(institute="MOHC")
print "Match:", rec

print "Create a file..."
f1 = DataFile(dataset=ds1, filename="tas_etc.nc", size=200, checksum="OIUDSIFDSFUO23432",
              download_url="http://www.hi", data_node="esgf-data1.ceda.ac.uk",
              variable="tas", cf_standard_name="air_temperature", variable_long_name="Temp of Air",
              start_time=datetime.datetime(2000, 1, 1),
              end_time=datetime.datetime(2100, 1, 1))
f1.save()

print "Number of files...", DataFile.objects.count()


            print 'Product: ', product
            print ("filename: %s \n dataset_id: %s \n version: %s \n download url: %s \n variable %s \n "
                   "variable cf name %s \n variable long name: %s \n variable units: %s \n experiment family: %s \n "
                   "product: %s \n filesize: %s \n forcings: %s \n checksum type: %s \n checksum: %s \n"
                   "tracking id: %s \n index node: %s \n replica: %s, institute: %s \n model: %s \n "
                   "experiment: %s \n frequency: %s \n realm: %s \n table: %s \n ensemble: %s \n filen: %s \n "
                   "start time: %s \n end time: %s \n ") % \
                  (filename, dataset_id, version, download_url, variable, variable_cf_name, variable_long_name,
                   variable_units, experiment_family, product, filesize, forcings, checksum_type, checksum,
                   tracking_id, index_node, replica, institute, model, experiment, frequency, realm, table, ensemble,
                   filen, start_time, end_time)

"""


if __name__ == '__main__':
    #create()
    datasets = Dataset.objects.all()
    for dataset in datasets:
        temporal_aggregation(dataset)
