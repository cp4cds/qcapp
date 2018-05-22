
import os
import shutil
import commands
from datetime import datetime

"""
/group_workspaces/jasmin2/cp4cds1/synda/sdt/data/
cmip5/output1/ICHEC/EC-EARTH/historical/mon/atmos/Amon/r14i1p1/v20171115/
pr_Amon_EC-EARTH_historical_r14i1p1_192001-192912.nc

/group_workspaces/jasmin2/cp4cds1/data
alpha/c3scmip5/output1/ICHEC/EC-EARTH/historical/mon/atmos/Amon/r14i1p1/pr/

files/20130315/pr_Amon_EC-EARTH_historical_r14i1p1_192001-192912.nc
v20130315/pr_Amon_EC-EARTH_historical_r14i1p1_198001-198912.nc -> ../files/20130315/pr_Amon_EC-EARTH_historical_r14i1p1_198001-198912.nc
latest -> v20130315
"""

sdt_base = "/group_workspaces/jasmin2/cp4cds1/synda/sdt_3.9.old/data/"
qc_base = "/group_workspaces/jasmin2/cp4cds1/data/alpha/c3scmip5/output1"
logfile = "new_version_check.log"
move_error_log = "move_error.log"


def move_new_files():
    with open(logfile) as reader:
        files = reader.readlines()

    move_files = []
    for line in files[:50]:
        if line.startswith('NEW'):
            move_files.append(line.strip())

    for file in move_files[1:]:
        rep_file, new_file = file.split(' :: ')[1:]

        print new_file
        # NEW FILES FILE
        if not os.path.exists(new_file):

            # NEW FILES DIR IF NEEDED
            files_version_dir = os.path.dirname(new_file)
            if not os.path.exists(files_version_dir):
                os.makedirs(files_version_dir)

            # THE COPY
            shutil.copy(rep_file, new_file)

        else:
            error_msg = "FILES FILE EXISTS NOT REPLACING :: {} :: {}\n".format(new_file, rep_file)
            with open(move_error_log, 'a+') as wr:
                wr.writelines(error_msg)
            pass

        # NEW VERSION DATAFILE DIR
        institute, model, experiment, frequency, realm, table, ensemble, variable, files, version_no, ncfile = new_file.split('/')[8:]
        version = 'v{}'.format(version_no)
        version_dir = os.path.join(qc_base, institute, model, experiment, frequency, realm, table, ensemble, variable, version)

        if not os.path.exists(version_dir):
            os.makedirs(version_dir)

        # NEW VERSION FILE
        version_file = os.path.join(version_dir, ncfile)
        if not os.path.exists(version_file):
            os.symlink(new_file, version_file)
        else:
            error_msg = "VERSION FILE EXISTS NOT SYMLINKING :: {} :: {}\n".format(new_file, version_file)
            with open(move_error_log, 'a+') as wr:
                wr.writelines(error_msg)
            pass

        # CHECK SYMLINK
        variable_dir = os.path.join(qc_base, institute, model, experiment, frequency, realm, table, ensemble, variable)
        os.chdir(variable_dir)
        old_version = datetime.strptime(os.readlink('latest').strip('v'), '%Y%m%d')
        new_version = datetime.strptime(version_no, '%Y%m%d')

        if new_version > old_version:
            os.remove('latest')
            os.symlink(version, 'latest')


def check_file_is_new(ifile):

    md5new = commands.getoutput('md5sum ' + ifile).split(' ')[0]
    institute, model, experiment, frequency, realm, table, ensemble, version, ncfile = ifile.split('/')[9:]
    variable = ncfile.split('_')[0]
    version_no = version.strip('v')
    new_files_dir = os.path.join(qc_base, institute, model, experiment, frequency, realm, table, ensemble,
                                 variable, "files", version_no)
    new_file = os.path.join(new_files_dir, ncfile)
    md5 = commands.getoutput('md5sum ' + new_file).split(' ')[0]

    if os.path.exists(new_file):
        if md5 == md5new :
            with open(logfile, 'a+') as writer:
             writer.writelines("EXISTS :: checksums match :: {} :: {}\n".format(ifile, new_file))
        else:
            with open(logfile, 'a+') as writer:
             writer.writelines("EXISTS :: checksums differ :: {} :: {}".format(ifile, new_file))
    else:
        with open(logfile, 'a+') as writer:
            writer.writelines("NEW FILE :: {} :: {}".format(ifile, new_file))

def check():

    counter = 0
    # domax = 5

    for root, dirs, files in os.walk(sdt_base):
        for name in files:
            # print(os.path.join(root, file))
            file = os.path.join(root, name)
            counter += 1

            check_file_is_new(file)

        #     if counter == domax:
        #         break
        # if counter == domax:
        #     break


if __name__ == "__main__":
    move_new_files()