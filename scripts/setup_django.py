"""
setup_django.py
===============
Makes connection to Django MOLES catalogue libraries/models.
"""

# Set up django interface
import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'qcproj.settings'

import django
django.setup()

# Imports from cedamoles_app
from qcapp.models import *
