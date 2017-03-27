from __future__ import unicode_literals

from django.db import models

# Create your models here.
class DataRequester(models.Model):
    """
    The CP4CDS data requester model.

    This holds the name of the project that specifics a subset of CMIP5 data for QC
    """
    requested_by = models.CharField(max_length=20)

class DataSpecification(models.Model):
    datarequesters = models.ManyToManyField(DataRequester, blank=True)
    variable = models.CharField(max_length=20)
    variable_long_name = models.CharField(max_length=80, blank=True, null=True, default='')
    cmor_table = models.CharField(max_length=20)
    frequency = models.CharField(max_length=20)
    priority = models.CharField(max_length=20, default='normal')
    esgf_data_collected = models.BooleanField(default=False)
    number_of_models = models.IntegerField(default=0)
    data_volume = models.FloatField(blank=True, null=True)

# TO DO WHEN QC IS COMPLETE
#    file_qc = models.ForeignKey('FileQC', null=True)
#    dataset_qc = models.ForeignKey('DatasetQC', null=True)

class Dataset(models.Model):

    data_spec = models.ManyToManyField(DataSpecification, blank=True)

    exists = models.BooleanField(default=False)
    project = models.CharField(max_length=30, default="cmip5")
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
    esgf_ds_id = models.CharField(max_length=200, blank=True)
    esgf_node = models.CharField(max_length=80, blank=True)


    # Generated from other facets when object is saved
    dataset_id = models.CharField(max_length=300, unique=True)

    def save(self, *args, **kwargs):
        dataset_id = "%s.%s.%s.%s.%s.%s.%s.%s.%s.%s.%s" % \
                     (self.project, self.product, self.institute, self.model, self.experiment, self.frequency,
                      self.realm, self.cmor_table, self.ensemble, self.variable, self.version)

        self.dataset_id = dataset_id
        super(Dataset, self).save(*args, **kwargs)


class DataFile(models.Model):

    dataset = models.ForeignKey(Dataset)

    filepath = models.CharField(max_length=300)
    archive_path = models.CharField(max_length=500)
    size = models.FloatField(blank=True)
    checksum = models.CharField(max_length=80)
    tracking_id = models.CharField(max_length=80, blank=True)
    download_url = models.CharField(max_length=300)
    variable = models.CharField(max_length=20)
    variable_long_name = models.CharField(max_length=80)
    cf_standard_name = models.CharField(max_length=300)
    variable_units = models.CharField(max_length=20)
    start_time = models.DateField()
    end_time = models.DateField()



class QCerror(models.Model):

    qc_check = models.ForeignKey(QCcheck)
    qc_error = models.CharField(max_length=300)


class QCcheck(models.Model):

    file_qc = models.ManyToManyField('FileQC')
    qc_check_type = models.CharField(max_length=20)


class FileQC(models.Model):

    check_file = models.ForeignKey(DataFile)
    cf_compliance_score = models.PositiveSmallIntegerField(default=0)
    ceda_cc_score = models.PositiveSmallIntegerField(default=0)
    file_qc_score = models.PositiveSmallIntegerField(default=0)


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


class QCresults(models.Model):
    dataset_qc = models.ForeignKey(DatasetQC)
    file_qc = models.ManyToManyField(FileQC)
    dataset_qc_score = DatasetQC.aggregate_score_of_ds_files
    file_qc_score = FileQC.file_qc_score
#   dataset_qc_link =
#   Many of these
#   Link to plot?

"""