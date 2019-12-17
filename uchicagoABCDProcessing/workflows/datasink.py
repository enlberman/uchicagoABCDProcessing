from smriprep.workflows.outputs import _bids_relative
from nipype import Workflow
from nipype.pipeline import engine as pe
from nipype.interfaces import utility as niu

from niworkflows.engine.workflows import LiterateWorkflow as Workflow
from ..interfaces import DerivativesDataSink

DEFAULT_MEMORY_MIN_GB=0.01


def init_regressed_datasink_wf(cold_output_dir: str, name='regressed_datasink_wf'):
    workflow = Workflow(name=name)

    inputnode = pe.Node(niu.IdentityInterface(fields=[
        'despiked', 'source_file']),
        name='inputnode')

    ds_regressed = pe.Node(DerivativesDataSink(
        base_directory=cold_output_dir, desc='clean', suffix='36p_despiked'),
        name="ds_regressed", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (inputnode, ds_regressed, [('source_file', 'source_file'),
                                   ('despiked', 'in_file'),
                                   ])
    ])

    return workflow


def init_derivatives_datasink_wf(hot_output_dir: str, atlas: str, name='datasink_wf'):
    workflow = Workflow(name=name)

    inputnode = pe.Node(niu.IdentityInterface(fields=[
        'despiked', 'transformed', 'hurst', 'hurst_ci', 'hurst_r2', 'connectivity']),
        name='inputnode')

    ds_atlas_transformed = pe.Node(DerivativesDataSink(
        base_directory=hot_output_dir, desc='ts', suffix=atlas,keep_dtype=True),
        name="ds_atlas", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (inputnode, ds_atlas_transformed, [('despiked', 'source_file'),
                                   ('transformed', 'in_file'),
                                   ])
    ])

    ds_hurst = pe.Node(DerivativesDataSink(
        base_directory=hot_output_dir, desc='atlas_dfa', suffix="hurst",keep_dtype=True),
        name="ds_hurst", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (ds_atlas_transformed, ds_hurst, [('out_file', 'source_file')]),
       (inputnode, ds_hurst,[('hurst', 'in_file')]),
    ])

    ds_hurst_ci = pe.Node(DerivativesDataSink(
        base_directory=hot_output_dir, desc='atlas_dfa', suffix="hurst_confidence_interval",keep_dtype=True),
        name="ds_hurst_ci", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (ds_atlas_transformed, ds_hurst_ci, [('out_file', 'source_file')]),
        (inputnode, ds_hurst_ci, [('hurst_r2', 'in_file')]),

    ])

    ds_hurst_r2 = pe.Node(DerivativesDataSink(
        base_directory=hot_output_dir, desc='atlas_dfa', suffix="hurst_rsquared",keep_dtype=True),
        name="ds_hurst_r2", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (ds_atlas_transformed, ds_hurst_r2, [('out_file', 'source_file')]),
        (inputnode, ds_hurst_r2,[('hurst_r2', 'in_file')]),
    ])

    ds_connectivity = pe.Node(DerivativesDataSink(
        base_directory=hot_output_dir, desc='atlas', suffix="connectivity",keep_dtype=True),
        name="ds_connectivity", run_without_submitting=True,
        mem_gb=DEFAULT_MEMORY_MIN_GB)
    workflow.connect([
        (ds_atlas_transformed, ds_connectivity, [('out_file', 'source_file')]),
        (inputnode, ds_connectivity, [('connectivity', 'in_file')]),
    ])
    return workflow
