#!/usr/bin/env python
"""
A driver routine that will run the quality control related routines specified.
"""
import argparse
from esgf_search import esgf_search
from db_builder import db_make
from run_quality_control import run_qc
from qc_error_fixer import fix_errors

parser = argparse.ArgumentParser()
parser.add_argument('variable', type=str, nargs='?', help='A CP4CDS variable')
parser.add_argument('frequency', type=str, nargs='?', help='A CP4CDS frequency')
parser.add_argument('table', type=str, nargs='?', help='A CP4CDS table')
parser.add_argument('--esgf_search',action='store_true', help='Search ESGF for variable information and cache results')
parser.add_argument('--db_make',action='store_true', help='Add the cached ESGF data to the QC database')
parser.add_argument('--run_qc',action='store_true', help='Run the quality control')
parser.add_argument('--fix_errors',action='store_true', help='Fix any qc errors')
parser.add_argument('--echo_inputs',action='store_true', help='Print out input args')


def echo_inputs(args):

    print("variable is {}".format(args.variable))
    print("frequency is {}".format(args.frequency))
    print("table is {}".format(args.table))

def main(args):

    if args.echo_inputs:
        echo_inputs(args)

    if args.esgf_search:
        esgf_search(args.variable, args.frequency, args.table)

    if args.db_make:
        db_make(args.variable, args.frequency, args.table)

    if args.run_qc:
        run_qc(args.variable, args.frequency, args.table)

    if args.fix_errors:
        fix_errors(args.variable, args.frequency, args.table)


if __name__ == "__main__":

    args = parser.parse_args()

    main(args)
