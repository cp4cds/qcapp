
from qc_settings import *
from qc_db_builder import *
from subprocess import call

#    node = "172.16.150.171"
experiments = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
experiments = ['rcp45']
file = "ancil_files/cp4cds_data_requirements.txt"
#file = "ancil_files/cp4cds_priority_data_requirements.txt"


with open(file, 'r') as reader:
    data = reader.readlines()

lineno = 0
for line in data:

    if lineno == 0:
        requester = line.split(',')[0].strip()
        if DEBUG: print requester

    if lineno > 1:
        variable = line.split(',')[0].strip()
        table = line.split(',')[1].strip()
        frequency = line.split(',')[2].strip()
        if DEBUG: print variable, table, frequency

        for experiment in experiments:

            lotus_cmd = ['./submit-lotus.sh', variable, table, frequency, experiment]
            print lotus_cmd
            res = call(lotus_cmd)

    lineno += 1



