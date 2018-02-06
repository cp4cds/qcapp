
import os
from utils import *
from esgf_dict import EsgfDict

def file_time_checks(ifile):


    institute, model, experiment, frequency, realm, table, ensemble, version, variable, ncfile = ifile.split('/')[6:]
    tc_odir = os.path.join(TC_DIR, institute, model, experiment, frequency, realm, version)

    if not os.path.exists(tc_odir):
        os.makedirs(tc_odir)

    single_file_time_checks(ifile, tc_odir)

