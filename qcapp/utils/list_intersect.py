# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '03 Oct 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


def list_intersect(list):
    return [[x for x in list[0] if x in list[1]]]


class VariableLongName:
    mapping = {
        'clt': 'Total Cloud Fraction',
        'evspsbl': 'Evaporation',
        'hfls': 'Surface Latent Heat Flux (upward)',
        'hfss': 'Surface Sensible Heat Flux (upward)',
        'hurs': 'Near-surface Relative Humidity',
        'huss': 'Near-surface Specific Humidity',
        'mrro': 'Total Runoff',
        'mrsos': 'Total Column Soil Moisture Content',
        'od550aer': 'Ambient Aerosol Optical Thickness at 550 nm',
        'pr': 'Precipitation',
        'prsn': 'Snowfall Flux',
        'ps': 'Surface Pressure',
        'psl': 'Sea-level Pressure',
        'rhs': 'Near Surface Relative Humidity',
        'rlds': 'Surface Downwelling Longwave Radiation',
        'rlus': 'Surface Upwelling Longwave Radiation',
        'rlut': 'Top of Atmosphere Outgoing Longwave Radiation',
        'rlutcs': 'Top of Atmosphere Outgoing Clear-sky Longwave Radiation',
        'rsds': 'Surface Downwelling Shortwave Radiation',
        'rsdt': 'Top of Atmosphere Incident Shortwave Radiation',
        'rsus': 'Surface Upwelling Shortwave Radiation',
        'rsut': 'Top of Atmosphere Outgoing Shortwave Radiation',
        'rsutcs': 'Top of Atmosphere Outgoing Clear-sky Shortwave Radiation',
        'sfcWind': 'Near-surface (10m) Wind Speed',
        'sic': 'Sea Ice Fraction',
        'sim': 'Sea Ice and Snow Amount',
        'sit': 'Sea Ice Thickness',
        'snd': 'Snow Depth Over Sea Ice',
        'snw': 'Surface Snow Amount',
        'sos': 'Sea Surface Salinity',
        'tas': 'Near-surface (2m) Air Temperature',
        'tasmax': 'Daily Maximum Near-surface Air Temperature',
        'tasmin': 'Daily Minimum Near-surface Air Temperature',
        'tauu': 'Surface Eastward Wind Stress (downward)',
        'tauv': 'Surface Northward Wind Stress (downward)',
        'tos': 'Sea Surface Temperature',
        'ts': 'Surface Skin Temperature',
        'tsice': 'Sea Ice Surface Temperature',
        'uas': 'Near-surface Zonal Component of Wind',
        'vas': 'Near-surface Meridional Component of Wind',
        'zos': 'Sea Surface Height Above Geoid',
        'hur': 'Relative Humidity',
        'hus': 'Specific Humidity',
        'ta': 'Air Temperature',
        'ua': 'Zonal Component of Wind',
        'va': 'Meridional Component of Wind',
        'zg': 'Geopotential Height'
    }

    @classmethod
    def get_longname(cls, var):
        return cls.mapping.get(var)
