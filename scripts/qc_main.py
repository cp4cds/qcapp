#!/usr/bin/env python
"""
A driver routine that will run the quality control related routines specified.
"""
import django
django.setup()

import os
import argparse
from sys import argv
from qcapp.models import *
from utils import *

parser = argparse.ArgumentParser()
parser.add_argument('--esgf_search',action='store_true', help='Search ESGF for variable information and cache results')
parser.add_argument('--db_add',action='store_true', help='Add the cached ESGF data to the QC database')
parser.add_argument('--run_qc',action='store_true', help='Run the quality control')


def main(args):

    if args.esgf_search:
        resolve_cedacc_exceptions()

    if args.db_add:
        update_cf_qc_error_record()

    if args.check_cedacc_output:
        update_cedacc_qc_errors()

    if args.run_qc:
        create_new_dataset_records()

if __name__ == "__main__":

    args = parser.parse_args()
    main(args)
