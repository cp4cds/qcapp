import django
django.setup()
import os
import datetime
from qcapp.models import *

logfile = "/group_workspaces/jasmin2/cp4cds1/qc/update-subset/dirs_with_multiple_versions.log"
delete_log = "test_delete.log"
database_log = "test_database_modifications.log"
log = "dataset_version_updater.log"
def get_original_version(dirs):

    vdirs = []
    for d in dirs:
        if d.startswith('v'):
            vdirs.append(d.strip('v'))

    vdirs_dt = []
    for vd in vdirs:
        vdirs_dt.append(datetime.datetime.strptime(vd, '%Y%m%d'))

    orig = "v" + datetime.datetime.strftime(min(vdirs_dt),'%Y%m%d')
    new = "v" + datetime.datetime.strftime(max(vdirs_dt),'%Y%m%d')
    return orig, new

def main():

    dup_dirs = set()
    duplicate_dfs = DataFile.objects.exclude(dataset__supersedes=None)
    for duplicate in duplicate_dfs:
        vardir = os.path.dirname(duplicate.gws_path).strip('latest').rstrip('/')
        dup_dirs.add(vardir)

    with open(log, 'a+') as w:

        # FIX FILE PATHS

        for dir in list(dup_dirs):
            os.chdir(dir)
            w.writelines(["DIR {} :: \n".format(os.getcwd())])
            dirs = os.listdir('.')
            orig_dir, new_dir = get_original_version(dirs)

            if os.path.exists(orig_dir) and os.path.exists(new_dir):
                w.writelines(["    VERSIONS :: {}, {}  \n".format(orig_dir, new_dir)])
                w.writelines(["    DELETING {} :: \n".format(new_dir)])
                os.removedirs(new_dir)
                w.writelines(["    DELETING {} :: \n".format('latest')])
                os.removedirs('latest')
                w.writelines(["    SYMLINKING {} to {} :: \n".format(orig_dir, 'latest')])
                os.symlink(orig_dir, 'latest')

            files_vdir = os.path.join('files', new_dir.strip('v'))

            if os.path.exists(files_vdir):
                w.writelines(["    DELETING {} :: \n".format(files_vdir)])
                os.removedirs(files_vdir)

            # # DATABASE MODIFICATIONS

            # # 1: Set new_dataset_version to False
            dfs = duplicate_dfs.filter(gws_path__startswith=dir)

            for df in dfs:
                df.new_dataset_version = False

                # 2: Set dataset = original dataset
                dsid = str(df.dataset)[:-10]
                dss = Dataset.objects.filter(dataset_id__startswith=dsid)
                orig_ds = dss.filter(version=orig_dir).first()
                new_ds = dss.filter(version=new_dir).first()
                w.writelines(["linked dataset {}\n".format(df.dataset)])
                df.dataset = orig_ds
                w.writelines(["reverted linked dataset {}\n".format(df.dataset)])
                df.save()

            # 3: Remove Dataset record that states supersedes
            w.writelines(["Deleting new ds {}\n".format(new_ds)])
            new_ds.delete()

if __name__ == "__main__":
    main()
