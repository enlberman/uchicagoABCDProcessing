from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterfaceInputSpec, SimpleInterface,
    File, InputMultiPath, OutputMultiPath)
from nipype import logging
from python_fractal_scaling.dfa import dfa
import pandas
import numpy
import nibabel
import nilearn
import cx_Oracle
import os
from nipype.utils.filemanip import fname_presuffix

LOGGER = logging.getLogger('nipype.interface')


class miNDARQueryInputSpec(TraitedSpec):
    subject_id = traits.String( mandatory=False, desc='subject identifier')
    username = traits.String(mandatory=True, desc='username for miNDAR')
    password = traits.String(mandatory=True, desc='passowrd for MINDAR')
    host = traits.String(mandatory=True, desc='host for miNDAR')


class miNDARQueryOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = traits.List(value=[])


class miNDARQueryDFA(SimpleInterface):
    """

    """
    input_spec = miNDARQueryInputSpec
    output_spec = miNDARQueryOutputSpec

    def _run_interface(self, runtime):
        connection = cx_Oracle
        img = nibabel.load(str(self.inputs.bold))
        tr = img.header.get('pixdim')[4]

        use_bold = self.inputs.brainmask is not None

        if use_bold:
            mask_img = nibabel.load(str(self.inputs.brainmask))
            data = nilearn.masking.apply_mask(imgs=img,mask_img=mask_img).T
        else:
            data = pandas.read_csv(str(self.inputs.csv))

        data = data[:,self.inputs.drop_vols:].T

        mn = int(numpy.ceil(1 / (tr * self.inputs.max_frequency)))
        mx = int(numpy.floor(1 / (tr * self.inputs.min_frequency)))

        h, hci, rs = dfa(data, max_window_size=mx, min_widow_size=mn)
        hci = numpy.vstack(hci)

        if use_bold:
            out_hurst = fname_presuffix(self.inputs.bold, suffix='_hurst', newpath=os.getcwd())
            out_cis = fname_presuffix(self.inputs.bold, suffix='_hurst_ci', newpath=os.getcwd())
            out_r2s = fname_presuffix(self.inputs.bold, suffix='_hurst_r2', newpath=os.getcwd())

            _bold_native_masked_derivative(bold_img=img, mask_img=mask_img, derivative_data=h, out_file=out_hurst)
            _bold_native_masked_derivative(bold_img=img, mask_img=mask_img, derivative_data=hci[:,0], out_file=out_cis)
            _bold_native_masked_derivative(bold_img=img, mask_img=mask_img, derivative_data=rs, out_file=out_r2s)
        else:
            out_hurst = fname_presuffix(self.inputs.bold, suffix='_hurst.csv', use_ext=False)
            out_cis = fname_presuffix(self.inputs.bold, suffix='_hurst_ci.csv', use_ext=False)
            out_r2s = fname_presuffix(self.inputs.bold, suffix='_hurst_r2.csv', use_ext=False)

            numpy.savetxt(out_hurst, h, delimiter=',')
            numpy.savetxt(out_cis, hci, delimiter=',')
            numpy.savetxt(out_r2s, rs, delimiter=',')

        self._results['hurst'] = out_hurst
        self._results['confidence_intervals'] = out_cis
        self._results['rsquared'] = out_r2s

        return runtime
