
from qc_settings import *
from qc_db_builder import *
from subprocess import call

#    node = "172.16.150.171"
expts = ['historical', 'piControl', 'amip', 'rcp26', 'rcp45', 'rcp60', 'rcp85']
debug = False
distrib = False
latest = True
# file = "ancil_files/cp4cds_priority_data_requirements.txt"
file = "ancil_files/cp4cds_data_requirements.txt"
#    node = "172.16.150.171"
node = "esgf-index1.ceda.ac.uk"

with open(file, 'r') as reader:
    data = reader.readlines()

lineno = 0
for line in data:

    if lineno == 0:
        requester = line.split(',')[0].strip()
        if debug: print requester

    if lineno > 1:
        variable = line.split(',')[0].strip()
        table = line.split(',')[1].strip()
        frequency = line.split(',')[2].strip()
        if debug: print variable, table, frequency

        # Add requester and request to tables and link up
        dRequester, _ = DataRequester.objects.get_or_create(requested_by=requester)
        dSpec, _ = DataSpecification.objects.get_or_create(variable=variable, cmor_table=table, frequency=frequency)
        dSpec.datarequesters.add(dRequester)
        dSpec.save()

    lineno += 1

    for experiment in experiments:

        lotus_cmd = ['bsub -o lotus-logs/%J -W 24:00 /group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/submit-lotus.sh',
                     variable, frequency, table, experiment]
        res = call(lotus_cmd)


