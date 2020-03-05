import os

import numpy
import pandas
from nipype.interfaces.base import (
    traits, TraitedSpec, SimpleInterface,
    File)
from nipype import logging
import cx_Oracle

LOGGER = logging.getLogger('nipype.interface')


class MotionInputSpec(TraitedSpec):
    bold = traits.String(mandatory=True, desc='bold_file')


class MotionOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = traits.File(exists=True, desc='motion file')


class Motion(SimpleInterface):
    """
    This is a simple interface for retrieving results of an sql query from an oracle database. The output is a pandas
    dataframe with query results. Note unless you build the column names into your query there are no column names
    in the resulting dataframe.
    """
    input_spec = MotionInputSpec
    output_spec = MotionOutputSpec

    def _run_interface(self, runtime):
        original_motion_file = self.inputs.bold.replace('_bold.nii', '_motion.tsv')
        motion_params = pandas.read_csv(original_motion_file,delimiter='\t')
        reordered_params = motion_params[['trans_x', 'trans_y', 'trans_z', 'rot_x', 'rot_y', 'rot_z']].values
        reordered_params[:, 3:] *= numpy.pi / 180
        output_file = os.path.join(os.getcwd(),'motion_params.txt')
        numpy.savetxt(output_file, reordered_params, delimiter=' ')

        self._results['out'] = output_file

        return runtime