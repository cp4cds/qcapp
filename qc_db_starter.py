
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
from qc_db_builder import *
from subprocess import call

#file = "ancil_files/cp4cds_data_requirements.txt"
#file = "ancil_files/magic_additional_data.txt"
#file = "ancil_files/cp4cds_priority_data_requirements.txt"

####
# Specify lotus output dir here
####
command_line_args = "--check_dataset_up_to_date"
lotus_out = 'latest_dataset_cache'
files = ["ancil_files/cp4cds_data_requirements.txt",
         "ancil_files/magic_additional_data.txt"]

if os.path.isfile(NO_FILE_LOG):
    os.remove(NO_FILE_LOG)
with open(NO_FILE_LOG, 'w') as fe:
    fe.write('')

for file in files:
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

                lotus_cmd = ['./submit-lotus.sh', variable, table, frequency, lotus_out, command_line_args]
                res = call(lotus_cmd)
                call(['sleep', '5'])
            lineno += 1



