
"""
Run this file to read in data request information in the form where the
first row must be the name of a project
second row must be headers approximating: variable short name, cmor table, frequency,
All subsequent rows must be the data where the first three columns must be in the order:
    variable short name, cmor table, frequency,
For each of these a separate lotus job will be run by calling
    "submit-lotus.sh" with the variable, table, frequency options

"""

from qc_settings import *
from subprocess import call

#file = "ancil_files/cp4cds_data_requirements.txt"
#file = "ancil_files/magic_additional_data.txt"
#file = "ancil_files/cp4cds_priority_data_requirements.txt"

####
# Specify lotus output dir here
####
#command_line_args = "--generate_latest_cache --dataset"

#command_line_args = "--is_latest_consistent"


command_line_args = "--parse_cf_checker" #""--multifile_time_check"
lotus_out = 'parse_cf_checker'
vars_file = "ancil_files/cp4cds_all_vars.txt"

delimiter = ','
with open(vars_file) as reader:
    data = reader.readlines()
    for line in data:
        variable = line.split(delimiter)[0].strip()
        frequency = line.split(delimiter)[1].strip()
        table = line.split(delimiter)[2].strip()
        # experiment = line.split(delimiter)[3].strip()
        # for experiment in ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']:
        lotus_cmd = ['./submit-lotus.sh', variable, frequency, table, lotus_out, command_line_args]
        res = call(lotus_cmd)

        # print variable, frequency, table
        # call(['sleep', '2'])


