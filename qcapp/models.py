from __future__ import unicode_literals

from django.db import models

class DataRequester(models.Model):
    """
    The CP4CDS data requester model.

    This holds the name of the project that specifics a subset of CMIP5 data for QC
    """
    requested_by = models.CharField(max_length=20)

    def __str__(self):
        return self.requested_by

class DataSpecification(models.Model):
    """
    The data specification
    """
    datarequesters = models.ManyToManyField(DataRequester, blank=True)
    variable = models.CharField(max_length=20)
    variable_long_name = models.CharField(max_length=80, blank=True, null=True, default='')
    cmor_table = models.CharField(max_length=20)
    frequency = models.CharField(max_length=20)
    priority = models.CharField(max_length=20, default='normal')
    esgf_data_collected = models.BooleanField(default=False)
    number_of_models = models.IntegerField(default=0)
    data_volume = models.FloatField(blank=True, null=True)

    def __str__(self):
        return self.variable_long_name


class Dataset(models.Model):
    """
    Dataset record
    """
    data_spec = models.ManyToManyField(DataSpecification, blank=True)

    exists = models.BooleanField(default=False)
    project = models.CharField(max_length=30, default="CMIP5")
    product = models.CharField(max_length=20)
    institute = models.CharField(max_length=30)
    model = models.CharField(max_length=20)
    experiment = models.CharField(max_length=20)
    frequency = models.CharField(max_length=20)
    realm = models.CharField(max_length=20)
    cmor_table = models.CharField(max_length=20)
    ensemble = models.CharField(max_length=10)
    version = models.CharField(max_length=10)
    start_time = models.DateField(blank=True, null=True)
    end_time = models.DateField(blank=True, null=True)
    variable = models.CharField(max_length=20)
    esgf_drs = models.CharField(max_length=200, blank=True)
    esgf_node = models.CharField(max_length=80, blank=True)
    up_to_date = models.NullBooleanField(default=None, blank=True, null=True)
    up_to_date_note = models.CharField(default=None, max_length=1000, blank=True, null=True)

    # TO DO WHEN QC IS COMPLETE
    #    dataset_qc = models.ForeignKey('DatasetQC', null=True)


    # Generated from other facets when object is saved
    dataset_id = models.CharField(max_length=300, unique=True)

    def save(self, *args, **kwargs):
        dataset_id = "%s.%s.%s.%s.%s.%s.%s.%s.%s.%s.%s" % \
                     (self.project, self.product, self.institute, self.model, self.experiment, self.frequency,
                      self.realm, self.cmor_table, self.ensemble, self.variable, self.version)

        self.dataset_id = dataset_id
        super(Dataset, self).save(*args, **kwargs)

    def __str__(self):
        return self.dataset_id


class DataFile(models.Model):
    """
    Dataset file
    """
    dataset = models.ForeignKey(Dataset)
    gws_path = models.CharField(max_length=500, default=None, blank=True, null=True)
    archive_path = models.CharField(max_length=500)
    ncfile = models.CharField(blank=True, max_length=300)
    size = models.FloatField(blank=True)
    sha256_checksum = models.CharField(max_length=80)
    md5_checksum = models.CharField(max_length=80, blank=True)
    tracking_id = models.CharField(max_length=80, blank=True)
    download_url = models.CharField(max_length=300)
    variable = models.CharField(max_length=20)
    variable_long_name = models.CharField(max_length=80)
    cf_standard_name = models.CharField(max_length=300)
    variable_units = models.CharField(max_length=20)
    start_time = models.DateField()
    end_time = models.DateField()
    published = models.NullBooleanField(default=None, blank=True, null=True)
    timeseries = models.NullBooleanField(default=None, blank=True, null=True)
    up_to_date = models.NullBooleanField(default=None, blank=True, null=True)
    up_to_date_note = models.CharField(default=None, max_length=1000, blank=True, null=True)
    restricted = models.NullBooleanField(default=None, blank=True, null=True)

    # Datafile QC information
    qc_passed = models.NullBooleanField(default=False, blank=True, null=True)
    cf_compliance_score = models.PositiveSmallIntegerField(default=0, blank=True)
    ceda_cc_score = models.PositiveSmallIntegerField(default=0, blank=True)
    file_qc_score = models.PositiveSmallIntegerField(default=0, blank=True)

    def __str__(self):
        return self.archive_path

class QCerror(models.Model):

    file = models.ForeignKey(DataFile, null=True)
    check_type = models.CharField(max_length=20, null=True, blank=True)
    error_type = models.CharField(max_length=20, null=True, blank=True)
    error_msg = models.CharField(max_length=800, null=True, blank=True)
    error_level = models.CharField(max_length=20, null=True, blank=True)
    report_filepath = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self):
        return self.report_filepath

"""

class DatasetQC(models.Model):

    check_dataset = models.ForeignKey(Dataset)
    # FOR TIME AXIS CHECK, CONTINUTIY AND COMPLETENESS
    qc_check = models.ManyToManyField(QCcheck, blank=True)

    aggregate_score_of_ds_files = models.SmallIntegerField()

class QCpercentiles(models.Model):

    dataset = models.ForeignKey(Dataset)
    qc_plot = models.ForeignKey('QCplot', null=True)

    project = models.CharField(max_length=30, default="cmip5")
    product = models.CharField(max_length=20)
    experiment = models.CharField(max_length=20)
    frequency = models.CharField(max_length=20)
    realm = models.CharField(max_length=20)
    cmor_table = models.CharField(max_length=20)
    model = models.CharField(max_length=20)
    ensemble = models.CharField(max_length=10)
    variable = models.CharField(max_length=20)

    max_value = models.DecimalField(decimal_places=1, max_digits=8)
    p999_value = models.DecimalField(decimal_places=1, max_digits=8)
    p990_value = models.DecimalField(decimal_places=1, max_digits=8)
    p950_value = models.DecimalField(decimal_places=1, max_digits=8)
    p750_value = models.DecimalField(decimal_places=1, max_digits=8)
    p500_value = models.DecimalField(decimal_places=1, max_digits=8)
    p250_value = models.DecimalField(decimal_places=1, max_digits=8)
    p050_value = models.DecimalField(decimal_places=1, max_digits=8)
    p010_value = models.DecimalField(decimal_places=1, max_digits=8)
    p001_value = models.DecimalField(decimal_places=1, max_digits=8)
    min_value = models.DecimalField(decimal_places=1, max_digits=8)
    ma_max_value = models.DecimalField(decimal_places=1, max_digits=8)
    ma_min_value = models.DecimalField(decimal_places=1, max_digits=8)


class QCplot(models.Model):

    dataset = models.ForeignKey(Dataset)
    qc_percentiles = models.ForeignKey(QCpercentiles)

    path = models.CharField(max_length=300, unique=True)
    project = models.CharField(max_length=30, default="cmip5")
    product = models.CharField(max_length=20)
    experiment = models.CharField(max_length=20)
    frequency = models.CharField(max_length=20)
    realm = models.CharField(max_length=20)
    cmor_table = models.CharField(max_length=20)
    ensemble = models.CharField(max_length=10)
    start_time = models.DateField(blank=True)
    end_time = models.DateField(blank=True)
    variable = models.CharField(max_length=20)


#class QCresults(models.Model):
#    dataset_qc = models.ForeignKey(DatasetQC)
#    file_qc = models.ManyToManyField(FileQC)
#    dataset_qc_score = DatasetQC.aggregate_score_of_ds_files
#    file_qc_score = FileQC.file_qc_score
#   dataset_qc_link =
#   Many of these
#   Link to plot?
"""

