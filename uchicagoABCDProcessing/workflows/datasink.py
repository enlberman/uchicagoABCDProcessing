from smriprep.workflows.outputs import _bids_relative
from nipype import Workflow
from nipype.pipeline import engine as pe
from nipype.interfaces import utility as niu

from niworkflows.engine.workflows import LiterateWorkflow as Workflow
from ..interfaces import DerivativesDataSink

DEFAULT_MEMORY_MIN_GB=0.01


def init_datasink_wf(bids_root: str, output_dir: str, name='datasink_wf'):
    workflow = Workflow(name=name)

    inputnode = pe.Node(niu.IdentityInterface(fields=[
        'dfa_h', 'dfa_ci', 'dfa_rsquared', 'source_file']),
        name='inputnode')

    raw_sources = pe.Node(niu.Function(function=_bids_relative), name='raw_sources')
    raw_sources.inputs.bids_root = bids_root

    ds_dfa_h = pe.Node(DerivativesDataSink(
        base_directory=output_dir, desc='dfa', suffix='hurst'),
        name="ds_dfa_h", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (inputnode, raw_sources, [('source_file', 'in_files')]),
        (inputnode, ds_dfa_h, [('source_file', 'source_file'),
                                   ('dfa_h', 'in_file'),
                               ])
    ])

    ds_dfa_ci = pe.Node(DerivativesDataSink(
        base_directory=output_dir, desc='dfa', suffix='ci'),
        name="ds_dfa_ci", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (inputnode, ds_dfa_ci, [('source_file', 'source_file'),
                               ('dfa_ci', 'in_file'),
                               ]),
    ])

    ds_dfa_r2 = pe.Node(DerivativesDataSink(
        base_directory=output_dir, desc='dfa', suffix='rsquared'),
        name="ds_dfa_rsquared", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (inputnode, ds_dfa_r2, [('source_file', 'source_file'),
                                ('dfa_rsquared', 'in_file'),
                                ]),
    ])

    return workflow