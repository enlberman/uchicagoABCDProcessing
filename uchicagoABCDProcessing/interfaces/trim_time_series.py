import os

import numpy
import pandas
from nipype.interfaces.base import (
    traits, TraitedSpec, SimpleInterface,
    File)
from nipype import logging


LOGGER = logging.getLogger('nipype.interface')


class TrimInputSpec(TraitedSpec):
    ts = traits.String(mandatory=True, desc='roi_time_series_file')


class TrimOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    trimmed = traits.File(exists=True, desc='trimmed time series')


class Trim(SimpleInterface):
    """
    This is a simple interface for retrieving results of an sql query from an oracle database. The output is a pandas
    dataframe with query results. Note unless you build the column names into your query there are no column names
    in the resulting dataframe.
    """
    input_spec = TrimInputSpec
    output_spec = TrimOutputSpec

    def _run_interface(self, runtime):
        original_ts_file = self.inputs.ts
        lower_file  = original_ts_file.lower()
        ts = numpy.loadtxt(original_ts_file, delimiter=',')
        new_length = 362 if lower_file.__contains__('nback') else 437 if lower_file.__contains__(
            'sst') else 403 if lower_file.__contains__('mid') else 375
        length = ts.shape[0]
        if length > new_length:
            trim_amount = length - new_length
            ts = ts[trim_amount:, :]
            numpy.savetxt(original_ts_file, ts, delimiter=',')

        self._results['trimmed'] = original_ts_file

        return runtime