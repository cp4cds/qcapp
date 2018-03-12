
import os
from qc_settings import *
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from utils import *



class EsgfDict(dict):

    def _format_gen_url(self, template, **kwargs):
        return template.format(**kwargs)

    def _generate_jsonfile_path(self, ds_obj, basedir, edict, dtype="dataset", ncfile=None, rw='r'):
        """

            _generate_local_logdir

        Uses a base directory with a database object (Dataset or DataFile) to build a file path.
        If "write mode" is selected the file path directories are constructed
        If the a sub-dircetory is selected an "experiment" sub-directory is included, for DataFile
        If an ncfile is specifed this is included as the path.

        :param basedir: A base directory
        :param ds_obj: A Django database object; Dataset
        :param edict: A esgf_dict object
        :param dtype: Set type to be dataset, or datafile, log at the experiment level for datafile
        :param rw: Read or Write mode
        :param ncfile: If DataFile object use the ncfile as the filename

        :return: Tuple of ([esgf_dict], [JSON-logfile])
        """

        if dtype not in ["dataset", "datafile"]:
            raise Exception("No Database object type specified")

        if dtype == "datafile":
            if ncfile == None:
                raise Exception("A netcdf file must be specifed for datafile type")

        if dtype == "dataset": logdir = basedir
        if dtype == "datafile": logdir = os.path.join(basedir, edict["experiment"])

        edict["institute"] = ds_obj.institute
        edict["model"] = ds_obj.model
        edict["realm"] = ds_obj.realm
        edict["ensemble"] = ds_obj.ensemble


        if dtype == "datafile":
            logfile = "{}.{}.json".format(ds_obj.project, ncfile.strip('.nc'))
        if dtype == "dataset":
            logfile = "{}.{}.json".format(ds_obj.esgf_drs, edict["variable"])

        json_file = os.path.join(logdir, logfile)

        if rw == 'w':
            if not os.path.isdir(logdir):
                os.makedirs(logdir)

        return edict, json_file


    def esgf_query(self, url, logfile):

        """

            esgf_query

        Perform an ESGF query based on the input url and log to the JSON file specified.

        :param url: ESGF URL query to ReST API
        :param logfile: JSON logfile
        """

        resp = requests.get(url, verify=False)
        json = resp.json()
        with open(logfile, 'w+') as fw:
            jsn.dump(json, fw)



    def format_is_latest_datafile_url(self):

        """

            format_is_latest_datafile_url

        Construct an is latest DataFile REST API URL query to ESGF

        :return: A latest, distributed, datafile, url query
        """

        template="https://{node}/esg-search/search?type=File&project={project}&time_frequency={frequency}&" \
                 "title={ncfile}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    frequency=self['frequency'],
                                    ncfile=self['ncfile'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


    def format_is_latest_dataset_url(self):

        """

            format_is_latest_dataset_url

        Construct an is latest Dataset REST API URL query to ESGF

        :return: A latest, distributed, dataset, url query
        """

        template="https://{node}/esg-search/search?type=Dataset&project={project}&institute={institute}&model={model}&" \
                 "experiment={experiment}&time_frequency={frequency}&realm={realm}&cmor_table={table}&ensemble={ensemble}&" \
                 "variable={variable}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    institute=self['institute'],
                                    model=self['model'],
                                    experiment=self['experiment'],
                                    frequency=self['frequency'],
                                    realm=self['realm'],
                                    table=self['table'],
                                    ensemble=self['ensemble'],
                                    variable=self['variable'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )
