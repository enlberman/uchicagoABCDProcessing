# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Interfaces to generate reportlets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

import os
import time
import re

from collections import Counter
from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterfaceInputSpec,
    File, Directory, InputMultiObject, Str, isdefined,
    SimpleInterface)
from nipype.interfaces import freesurfer as fs
from niworkflows.utils.bids import BIDS_NAME


SUBJECT_TEMPLATE = """\
\t<ul class="elem-desc">
\t\t<li>Subject ID: {subject_id}</li>
\t\t<li>Structural images: {n_t1s:d} T1-weighted {t2w}</li>
\t\t<li>Functional series: {n_bold:d}</li>
{tasks}
\t\t<li>Other series: {n_other:d}</li>
{tasks}
\t</ul>
"""

ABOUT_TEMPLATE = """\t<ul>
\t\t<li>uchicagoABCDProcessing version: {version}</li>
\t\t<li>neuroHurrst command: <code>{command}</code></li>
\t\t<li>Date preprocessed: {date}</li>
\t</ul>
</div>
"""


class SummaryOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='HTML segment containing summary')


class SummaryInterface(SimpleInterface):
    output_spec = SummaryOutputSpec

    def _run_interface(self, runtime):
        segment = self._generate_segment()
        fname = os.path.join(runtime.cwd, 'report.html')
        with open(fname, 'w') as fobj:
            fobj.write(segment)
        self._results['out_report'] = fname
        return runtime

    def _generate_segment(self):
        raise NotImplementedError


class SubjectSummaryInputSpec(BaseInterfaceInputSpec):
    t1w = InputMultiObject(File(exists=True), desc='T1w structural images')
    t2w = InputMultiObject(File(exists=True), desc='T2w structural images')
    subjects_dir = Directory(desc='FreeSurfer subjects directory')
    subject_id = Str(desc='Subject ID')
    bold = InputMultiObject(traits.Either(
        File(exists=True), traits.List(File(exists=True))),
        desc='BOLD functional series')
    other_ts = InputMultiObject(traits.Either(
        File(exists=True), traits.List(File(exists=True))),
        desc='Other series')


class SubjectSummaryOutputSpec(SummaryOutputSpec):
    # This exists to ensure that the summary is run prior to the first ReconAll
    # call, allowing a determination whether there is a pre-existing directory
    subject_id = Str(desc='FreeSurfer subject ID')


class SubjectSummary(SummaryInterface):
    input_spec = SubjectSummaryInputSpec
    output_spec = SubjectSummaryOutputSpec

    def _run_interface(self, runtime):
        if isdefined(self.inputs.subject_id):
            self._results['subject_id'] = self.inputs.subject_id
        return super(SubjectSummary, self)._run_interface(runtime)

    def _generate_segment(self):
        t2w_seg = ''
        if self.inputs.t2w:
            t2w_seg = '(+ {:d} T2-weighted)'.format(len(self.inputs.t2w))

        # Add list of tasks with number of runs
        bold_series = self.inputs.bold if isdefined(self.inputs.bold) else []
        bold_series = [s[0] if isinstance(s, list) else s for s in bold_series]

        other_series = self.inputs.other_ts if isdefined(self.inputs.other_ts) else []
        other_series = [s[0] if isinstance(s, list) else s for s in other_series]

        counts = Counter(BIDS_NAME.search(series).groupdict()['task_id'][5:]
                         for series in bold_series)

        if not bold_series:
            counts = Counter(BIDS_NAME.search(series).groupdict()['task_id'][5:]
                             for series in other_series)

        tasks = ''
        if counts:
            header = '\t\t<ul class="elem-desc">'
            footer = '\t\t</ul>'
            lines = ['\t\t\t<li>Task: {task_id} ({n_runs:d} run{s})</li>'.format(
                     task_id=task_id, n_runs=n_runs, s='' if n_runs == 1 else 's')
                     for task_id, n_runs in sorted(counts.items())]
            tasks = '\n'.join([header] + lines + [footer])

        return SUBJECT_TEMPLATE.format(
            subject_id=self.inputs.subject_id,
            n_t1s=len(self.inputs.t1w),
            t2w=t2w_seg,
            n_bold=len(bold_series),
            n_other=len(other_series),
            tasks=tasks)


class FunctionalSummaryInputSpec(BaseInterfaceInputSpec):
    slice_timing = traits.Enum(False, True, 'TooShort', usedefault=True,
                               desc='Slice timing correction used')
    distortion_correction = traits.Str(desc='Susceptibility distortion correction method',
                                       mandatory=True)
    pe_direction = traits.Enum(None, 'i', 'i-', 'j', 'j-', mandatory=True,
                               desc='Phase-encoding direction detected')
    registration = traits.Enum('FSL', 'FreeSurfer', mandatory=True,
                               desc='Functional/anatomical registration method')
    fallback = traits.Bool(desc='Boundary-based registration rejected')
    registration_dof = traits.Enum(6, 9, 12, desc='Registration degrees of freedom',
                                   mandatory=True)
    confounds_file = File(exists=True, desc='Confounds file')
    tr = traits.Float(desc='Repetition time', mandatory=True)
    dummy_scans = traits.Either(traits.Int(), None, desc='number of dummy scans specified by user')
    algo_dummy_scans = traits.Int(desc='number of dummy scans determined by algorithm')


class AboutSummaryInputSpec(BaseInterfaceInputSpec):
    version = Str(desc='uchicagoABCDProcessing version')
    command = Str(desc='uchicagoABCDProcessing command')
    # Date not included - update timestamp only if version or command changes


class AboutSummary(SummaryInterface):
    input_spec = AboutSummaryInputSpec

    def _generate_segment(self):
        return ABOUT_TEMPLATE.format(version=self.inputs.version,
                                     command=self.inputs.command,
                                     date=time.strftime("%Y-%m-%d %H:%M:%S %z"))