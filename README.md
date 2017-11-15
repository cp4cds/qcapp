## The Climate projections for the Copernicus Data Store (CP4CDS) Quality Control App code base

# The QC app

The QC application will use the tools:
* CEDA-CC
* CF-checker
* Additional routines built here
  * Time checks

to provide a QC summary for all the data in the CP4CDS qcapp


# Using the QC app on lotus

1. Checkout
2. `export DJANGO_SETTINGS_MODULE=qcproj.settings`
3. To run a parallel process
* Run `qc_db_starter.py`
* This reads in a "data request"
* For each variable, at a given frequency and CMOR table this will set off a lotus process by calling `submit-lotus.sh`
    * `submit-lotush.sh` submits jobs to louts through `run_qc_lotus.sh`
    * `run_qc_lotus.sh` sets up the correct environment on the lotus node and calls `qc_db_builder.py`
    * `qc_db_builder.py` takes the three arguments: variable, cmor_table, frequency and applies the selected functions for all experiments:
    historical, piControl, amip, rcp26, rcp45, rcp60, rcp85


4. To run a single process currently need to call with args `qc_db_builder.py <var> <table> <frequency>`



# `qc_db_builder.py`

* Populates tables from an esgf-index1.ceda.ac.uk local data node only search:
    1. DataRequester
    2. DataSpecification
    3. Dataset
    4. DataFile

Also
* Calculates the local md5sum
* Checks if the directory holding the data has more than one file, i.e. data is part of a timeseries

* Performs a distributed search, for latest, no replicas to checks whether this is the most recent version of the data


# Corrections
The time checker will adjust automatically any NetCDF file where the units of time are given as "days since YYYY-MM"
to "days since YYYY-MM-DD".

If a file start date is 0001-01-01 then the time checker performs only one consistency check

If the file is valid at one time only then in time_checks/file_time_checks.py if len(times) == 1, returns True