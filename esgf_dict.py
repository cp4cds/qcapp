
import os
from qc_settings import *
from time_checks.run_file_timechecks import main as single_file_time_checks
from time_checks.run_multifile_timechecks import main as multi_file_time_checks
from utils import *



class EsgfDict(dict):

    def _format_gen_url(self, template, **kwargs):
        return template.format(**kwargs)

    def _generate_local_logdir(self, basedir, ds, edict, subdir=None, rw='r'):

        edict["institute"] = ds.institute
        edict["model"] = ds.model
        edict["realm"] = ds.realm
        edict["ensemble"] = ds.ensemble

        if subdir == None: logdir = basedir
        if subdir == "exper": logdir = os.path.join(basedir, edict["experiment"])

        logfile = ds.esgf_drs + ".json"
        json_file = os.path.join(logdir, logfile)

        if rw == 'w':
            if not os.path.isdir(logdir):
                os.makedirs(logdir)

        return edict, json_file


    def esgf_query(self, url, logfile):
        resp = requests.get(url, verify=False)
        json = resp.json()
        with open(logfile, 'w') as fw:
            jsn.dump(json, fw)



    def format_is_latest_datafile_url(self):
        template="https://{node}/esg-search/search?type=File&project={project}&institute={institute}&" \
                 "time_frequency={frequency}&realm={realm}&title={ncfile}&distrib={distrib}&latest={latest}" \
                 "&format=application%2Fsolr%2Bjson&limit=10000"

        return self._format_gen_url(template,
                                    node=self['node'],
                                    project=self['project'],
                                    institute=self['institute'],
                                    frequency=self['frequency'],
                                    realm=self['realm'],
                                    ncfile=self['ncfile'],
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )


    def format_is_latest_dataset_url(self):

        template="https://{node}/esg-search/search?type=Dataset&project={project}&institute={institute}&model={model}&" \
                 "experiment={experiment}&time_frequency={frequency}&realm={realm}&cmor_table={table}&ensemble={ensemble}&" \
                 "distrib={distrib}&latest={latest}" \
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
                                    distrib=self['distrib'],
                                    latest=self['latest'],
                                    )
