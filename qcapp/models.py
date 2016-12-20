from __future__ import unicode_literals

from django.db import models


# Create your models here.
class Dataset(models.Model):

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

    experiment_family = models.CharField(max_length=100, blank=True)
    forcing = models.CharField(max_length=500, blank=True)
    start_time = models.DateField(blank=True)
    end_time = models.DateField(blank=True)
    variable = models.CharField(max_length=20)

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

    filename = models.CharField(max_length=300, unique=True)
    size = models.BigIntegerField()
    checksum = models.CharField(max_length=80)
    tracking_id = models.CharField(max_length=80)
    download_url = models.CharField(max_length=300)

    variable = models.CharField(max_length=20)
    variable_long_name = models.CharField(max_length=80)
    cf_standard_name = models.CharField(max_length=300)
    variable_units = models.CharField(max_length=20)

    data_node = models.URLField()
    start_time = models.DateField()
    end_time = models.DateField()


