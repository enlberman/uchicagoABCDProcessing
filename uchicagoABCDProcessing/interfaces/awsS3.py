import os

from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterfaceInputSpec, SimpleInterface,
    File, InputMultiPath, OutputMultiPath)
from nipype import logging
import boto3
import boto3.session

LOGGER = logging.getLogger('nipype.interface')


class awsS3InputSpec(TraitedSpec):
    link = traits.String( mandatory=False, desc='s3 link')
    dest = traits.String( mandatory=False, desc='destination path')
    access_key = traits.String( mandatory=False, desc='access key')
    secret_access_key = traits.String( mandatory=False, desc='secret access key')

class awsS3OutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = File(exists=True, desc='downloaded file')


class s3Download(SimpleInterface):
    """

    """
    input_spec = awsS3InputSpec
    output_spec = awsS3OutputSpec

    def _run_interface(self, runtime):
        out=None
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.inputs.access_key,
                aws_secret_access_key=self.inputs.secret_access_key
            )

            link = self.inputs.link.split('/')
            bucket = link[2]
            file = self.inputs.link.split(bucket+'/')[1]
            """ e.g.
            self.inputs.link = 's3://NDAR_Central_2/submission_19173/NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz'
            link = ['s3:', '', 'NDAR_Central_2', 'submission_19173', 'NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz']
            bucket = 'NDAR_Central_2'
            file = 'submission_19173/NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz'
            """

            s3.download_file(bucket, file, self.inputs.dest)

        except Exception as e:
            print("There was a problem with Oracle: ",  e)
        finally:
            if s3:
                s3.close()

        self._results['out'] = out

        return runtime
