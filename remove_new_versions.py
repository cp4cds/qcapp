import django
django.setup()
import os
import datetime
import shutil
from qcapp.models import *

logfile = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/multiple_version_dirs_1.log"
# logfile = "/group_workspaces/jasmin2/cp4cds1/qc/update-subset/dirs_with_multiple_versions.log"
# delete_log = "test_delete.log"
# database_log = "test_database_modifications.log"
log = "/group_workspaces/jasmin2/cp4cds1/qc/qc-app2/qcapp/dataset_version_reverter-11.log"
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

    # dup_dirs = set()
    # duplicate_dfs = DataFile.objects.exclude(dataset__supersedes=None)
    # for duplicate in duplicate_dfs:
    #     vardir = os.path.dirname(duplicate.gws_path).strip('latest').rstrip('/')
    #     dup_dirs.add(vardir)

    with open(logfile) as r:
        dirs = r.readlines()


    with open(log, 'a+') as w:

        # FIX FILE PATHS

        # for dir in list(dup_dirs):
        for dir in dirs:
            dir = dir.strip()
            os.chdir(dir)

            msg = "DIR {} :: \n".format(os.getcwd())
            # print msg
            w.writelines(msg)

            dirs = os.listdir('.')
            if len(dirs) > 3:
                print "more than three dirs {}".format(os.listdir('.'))
                orig_dir, new_dir = get_original_version(dirs)

                if os.path.exists(orig_dir) and os.path.exists(new_dir):
                    msg = "    VERSIONS :: {}, {}  \n".format(orig_dir, new_dir)
                    # print msg
                    w.writelines(msg)

                    msg = "    DELETING {} :: \n".format(new_dir)
                    # print msg
                    w.writelines(msg)

                    shutil.rmtree(new_dir, ignore_errors=True)

                    msg = "    DELETING {} :: \n".format('latest')
                    # print msg
                    w.writelines(msg)

                    os.unlink('latest')

                    msg = "    SYMLINKING {} to {} :: \n".format(orig_dir, 'latest')
                    # print msg
                    w.writelines(msg)

                    os.symlink(orig_dir, 'latest')
                files_vdir = os.path.join('files', new_dir.strip('v'))

                if os.path.exists(files_vdir):
                    msg = "    DELETING {} :: \n".format(files_vdir)
                    # print msg
                    w.writelines(msg)
                    shutil.rmtree(files_vdir, ignore_errors=True)

                # # DATABASE MODIFICATIONS

                # # 1: Set new_dataset_version to False
                dfs = DataFile.objects.filter(gws_path__startswith=dir)

                if dfs.count() > 0:
                    for df in dfs:
                        df.new_dataset_version = False

                        # 2: Set dataset = original dataset
                        dsid = str(df.dataset)[:-10]
                        dss = Dataset.objects.filter(dataset_id__startswith=dsid)
                        orig_ds = dss.filter(version=orig_dir).first()
                        new_ds = dss.filter(version=new_dir).first()#

                        msg = "linked dataset {}\n".format(df.dataset)
                        # print msg
                        w.writelines(msg)
                        df.dataset = orig_ds

                        msg = "reverted linked dataset {}\n".format(df.dataset)
                        # print msg
                        w.writelines(msg)
                        df.save()

                    # 3: Remove Dataset record that states supersedes
                    try:
                        msg = "Deleting new ds {}\n".format(new_ds)
                        # print msg
                        w.writelines(msg)
                        new_ds.delete()
                    except:
                        continue
            else:
                print "Not doing anything"
if __name__ == "__main__":
    main()
