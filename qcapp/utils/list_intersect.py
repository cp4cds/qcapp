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