import os
from pathlib import Path

from nipype.interfaces.base import (
    traits, TraitedSpec, SimpleInterface,
    File)
from nipype import logging
import boto3
import boto3.session

LOGGER = logging.getLogger('nipype.interface')


class awsS3InputSpec(TraitedSpec):
    links = traits.List( mandatory=True, desc='s3 links')
    dest = traits.String( mandatory=True, desc='destination path')
    access_key = traits.String( mandatory=True, desc='access key')
    secret_access_key = traits.String( mandatory=True, desc='secret access key')


class awsS3OutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = traits.String(exists=True, desc='downloaded files directory')


class s3Download(SimpleInterface):
    """
        download each of the input files one at a time to the dest folder. To download files to multiple folders use
        separate instances of this interface
    """
    input_spec = awsS3InputSpec
    output_spec = awsS3OutputSpec

    def _run_interface(self, runtime):
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.inputs.access_key,
            aws_secret_access_key=self.inputs.secret_access_key
        )
        files = []

        for link in self.inputs.links:
            link_list = link.split('/')
            bucket = link_list[2]
            file = link.split(bucket+'/')[1]
            files.append(os.path.join(self.inputs.dest,file))
            """ e.g.
            self.inputs.link = 's3://NDAR_Central_2/submission_19173/NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz'
            link = ['s3:', '', 'NDAR_Central_2', 'submission_19173', 'NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz']
            bucket = 'NDAR_Central_2'
            file = 'submission_19173/NDARINVBL4HN6F4_baselineYear1Arm1_ABCD-MPROC-DTI_20180525132204.tgz'
            """

            s3.download_file(bucket, file, self.inputs.dest)

        if sum([Path(f).exists for f in files]):  # only have an output if all the files were actually downloaded
            self._results['out'] = self.inputs.dest

        return runtime
