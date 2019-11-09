#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
This pipeline is developed by the Environmental Neuroscience Lab at the University of Chicago
(https://enl.uchicago.edu/) for open-source software distribution.
"""

from .__about__ import (  # noqa
    __version__,
    __copyright__,
    __credits__,
    __packagename__,
)

import warnings

# cmp is not used by uchicagoABCDProcessing, so ignore nipype-generated warnings
warnings.filterwarnings('ignore', r'cmp not installed')
warnings.filterwarnings('ignore', r'This has not been fully tested. Please report any failures.')
warnings.filterwarnings('ignore', r"can't resolve package from __spec__ or __package__")
warnings.simplefilter('ignore', DeprecationWarning)
warnings.simplefilter('ignore', ResourceWarning)
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
