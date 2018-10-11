
from setup_django import *
from settings import *
from subprocess import call

command_line_args = "--fix_errors"
run = "sos-qc-fixes"
lotus_log_dir = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app-dev/lotus-logs/{}".format(run)
variable = 'sos'
frequency = 'mon'
table = 'Omon'

# for experiment in ALLEXPTS:
#
#     datasets = Dataset.objects.filter(variable=variable, frequency=frequency, cmor_table=table,
#                                       experiment=experiment)
#     models = list(datasets.values_list('model', flat=True).distinct())
#     for model in models:
experiment = 'historical'
model = 'CNRM-CM5'
lotus_cmd = ['./submit-lotus.sh', variable, frequency, table, experiment, model, lotus_log_dir, command_line_args]
res = call(lotus_cmd)