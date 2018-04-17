
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


command_line_args = "--ceda_cc"
lotus_out = 'run-ceda-cc'
vars_file = "ancil_files/cp4cds_all_vars.txt"
vars_file = "ceda_cc_redo.log"

delimiter = ' '
with open(vars_file) as reader:
    data = reader.readlines()
    for line in data:
        variable = line.split(delimiter)[0].strip()
        frequency = line.split(delimiter)[1].strip()
        table = line.split(delimiter)[2].strip()
        # print variable, frequency, table

        lotus_cmd = ['./submit-lotus.sh', variable, frequency, table, lotus_out, command_line_args]
        res = call(lotus_cmd)
        # call(['sleep', '2'])


# if frequency == "mon":
#     if variable in ["tas", "ts", "tasmax", "tasmin", "psl", "ps", "uas", "vas", "sfcWind", "hurs",
#                     "huss", "pr", "prsn", "evspsbl", "tauu", "tauv", "hfls", "hfss", "rlds", "rlus",
#                     "rsds", "rsus", "rsdt", "rsut", "rlut", "clt", "tos", "zos", "ta", "ua", "va",
#                     "hur", "zg", "rlutcs"]:
