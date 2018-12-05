from setup_django import *
import os
import sys
import datetime
import re
import glob
import json
import commands
from settings import *
import utils
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()

certificate = '/home/users/rpetrie/.globus/certificate-file'
odir = '/group_workspaces/jasmin2/cp4cds1/data/new_version'


def generate_new_files_list():

    update_files_errors = QCerror.objects.filter(check_type='LATEST', error_level='UPDATE')
    for err in update_files_errors[:1]:

        regex = r"http://.*nc.*HTTP"
        url = re.findall(regex, err.error_msg)
        if url:
            if not len(url) == 1:
                continue
            else:
                print url[0].split('|')[0]

        ofile = os.path.join(odir, url.split('/')[-1])
        if not os.path.exists(ofile):
            try:
                # cmd = ['wget', '-nv', '--certificate', certificate, '--no-check-certificate', url, '-O', ofile]
                cmd = ['wget', '-nv', '--no-check-certificate', url, '-O', ofile]
                subprocess.call(cmd)
                print("RETRIEVED file :: {}".format(ofile))
            except:
                print("FAILED to retrieve :: {}".format(url))

        # msg_comps = err.error_msg.split(' : ')
        # for comp in msg_comps:
        #     if "HTTPServer" in comp:



if __name__ == "__main__":
    generate_new_files_list()