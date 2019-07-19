
from setup_django import *
import os
import shutil



def check(ds):
    pass


if __name__ == "__main__":

    with open('datasets_in_psql_to_republish') as r:
        datasets = [line.strip() for line in r]

    with open('timeseries_error_dataset_ids') as r:
        ts_errors = [line.strip() for line in r]

    for dsid in datasets:
        if "fx" in dsid:
            continue
        ds = Dataset.objects.filter(dataset_id__icontains='.'.join(dsid.split('.')[1:-1])).exclude(version='v20181201').first()
        if not ds:
            continue
        if ds in ts_errors:
            continue

        for df in ds.datafile_set.all():
            errors = df.qcerror_set.exclude(error_msg__icontains='ERROR (4)')
            if not len(errors) == 0:
                print "datafile errors",
                for e in errors:
                    print e.error_msg
                with open('datasets_with_datafile_errors', 'a+') as w:
                    w.writelines(["{}\n".format(dsid)])
                continue
        # ds_errors = ds.qcerror_set.all()
        #
        # if not len(ds_errors) == 0:
        #     print "dataset errors", ds, ds_errors, len(ds_errors)