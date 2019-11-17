from argparse import ArgumentParser
from bids import BIDSLayout
from nipype import Workflow
import sys
import os
from copy import deepcopy
from fmriprep.workflows.base import init_fmriprep_wf

from nipype import __version__ as nipype_ver
from nipype.pipeline import engine as pe
from nipype.interfaces import utility as niu

from niworkflows.engine.workflows import LiterateWorkflow as Workflow
from niworkflows.interfaces.bids import (
    BIDSInfo
)

from neuroHurst.workflows.datasink import init_datasink_wf
from ..utils.bids import collect_data, BIDSPlusDataGrabber

from ..workflows.exampleWorkflow import init_dfa_workflow

from ..interfaces import SubjectSummary, AboutSummary, DerivativesDataSink
from ..__about__ import __version__


def init_base_wf(
        anat_only,
        aroma_melodic_dim,
        bold2t1w_dof,
        cifti_output,
        debug,
        dummy_scans,
        echo_idx,
        err_on_aroma_warn,
        fmap_bspline,
        fmap_demean,
        force_syn,
        freesurfer,
        hires,
        ignore,
        layout,
        longitudinal,
        low_mem,
        medial_surface_nan,
        omp_nthreads,
        output_dir,
        output_spaces,
        run_uuid,
        regressors_all_comps,
        regressors_fd_th,
        regressors_dvars_th,
        skull_strip_fixed_seed,
        skull_strip_template,
        subject_list,
        t2s_coreg,
        task_id,
        use_aroma,
        use_bbr,
        use_syn,
        work_dir,
        opts: ArgumentParser):
    fmriprep_workflow = init_fmriprep_wf(
        anat_only=anat_only,
        aroma_melodic_dim=aroma_melodic_dim,
        bold2t1w_dof=bold2t1w_dof,
        cifti_output=cifti_output,
        debug=debug,
        dummy_scans=dummy_scans,
        echo_idx=echo_idx,
        err_on_aroma_warn=err_on_aroma_warn,
        fmap_bspline=fmap_bspline,
        fmap_demean=fmap_demean,
        force_syn=force_syn,
        freesurfer=freesurfer,
        hires=hires,
        ignore=ignore,
        layout=layout,
        longitudinal=longitudinal,
        low_mem=low_mem,
        medial_surface_nan=medial_surface_nan,
        omp_nthreads=omp_nthreads,
        output_dir=str(output_dir),
        output_spaces=output_spaces,
        run_uuid=run_uuid,
        regressors_all_comps=regressors_all_comps,
        regressors_fd_th=regressors_fd_th,
        regressors_dvars_th=regressors_dvars_th,
        skull_strip_fixed_seed=skull_strip_fixed_seed,
        skull_strip_template=skull_strip_template,
        subject_list=subject_list,
        t2s_coreg=t2s_coreg,
        task_id=task_id,
        use_aroma=use_aroma,
        use_bbr=use_bbr,
        use_syn=use_syn,
        work_dir=str(work_dir),
    )
    workflow = Workflow(name='uchicagoABCDProcessing_wf')
    workflow.base_dir = opts.work_dir

    reportlets_dir = os.path.join(opts.work_dir, 'reportlets')
    for subject_id in subject_list:
        single_subject_wf = init_single_subject_wf(
            opts=opts,
            layout=layout,
            run_uuid=run_uuid,
            work_dir=str(work_dir),
            output_dir=str(output_dir),
            name="single_subject_" + subject_id + "_wf",
            subject_id=subject_id,
            reportlets_dir=reportlets_dir,
        )

        single_subject_wf.config['execution']['crashdump_dir'] = (
            os.path.join(output_dir, "uchicagoABCDProcessing", "sub-" + subject_id, 'log', run_uuid)
        )
        for node in single_subject_wf._get_all_nodes():
            node.config = deepcopy(single_subject_wf.config)

        workflow.add_nodes([single_subject_wf])

    return workflow


def init_single_subject_wf(
        opts: ArgumentParser,
        layout: BIDSLayout,
        run_uuid: str,
        work_dir: str,
        output_dir: str,
        name: str,
        subject_id: str,
        reportlets_dir: str,
):
    import nilearn
    if name in ('single_subject_wf', 'single_subject_test_wf'):
        # for documentation purposes
        subject_data = {
            'bold': ['/completely/made/up/path/sub-01_task-nback_bold.nii.gz']
        }
    else:
        subject_data = collect_data(layout, subject_id, opts.source_format)[0]

    if not subject_data[opts.source_format]:
        raise Exception("No {data_format}. data found for participant {participant}. "
                        "All workflows require time series data.".format(
            data_format=opts.source_format,
            participant=subject_id)
        )

    workflow = Workflow(name=name)
    workflow.__desc__ = """
    Results included in this manuscript come from processing
    performed using *uchicagoABCDProcessing* {uchicagoABCDProcessing_ver}
    (@uchicagoABCDProcessing1; @uchicagoABCDProcessing; RRID:some.id),
    which is based on *Nipype* {nipype_ver}
    (@nipype1; @nipype2; RRID:SCR_002502).
    """.format(uchicagoABCDProcessing_ver=__version__, nipype_ver=nipype_ver)
    workflow.__postdesc__ = """
    Many internal operations of *uchicagoABCDProcessing* use
    *Nilearn* {nilearn_ver} [@nilearn, RRID:SCR_001362].
    ### Copyright Waiver
    The above boilerplate text was automatically generated by fMRIPrep
    with the express intention that users should copy and paste this
    text into their manuscripts *unchanged*.
    It is released under the [CC0]\
    (https://creativecommons.org/publicdomain/zero/1.0/) license.
    ### References
    """.format(nilearn_ver=nilearn.version.__version__)

    inputnode = pe.Node(niu.IdentityInterface(fields=['subjects_dir']),
                        name='inputnode')

    require_masks = opts.source_format == 'bold'
    bidssrc = pe.Node(BIDSPlusDataGrabber(subject_data=subject_data, require_masks=require_masks),
                      name='bidssrc')

    bids_info = pe.Node(BIDSInfo(
        bids_dir=layout.root, bids_validate=False), name='bids_info')

    summary = pe.Node(SubjectSummary(),
                      name='summary', run_without_submitting=True)

    about = pe.Node(AboutSummary(version=__version__,
                                 command=' '.join(sys.argv)),
                    name='about', run_without_submitting=True)

    ds_report_summary = pe.Node(
        DerivativesDataSink(base_directory=reportlets_dir,
                            desc='summary', keep_dtype=True),
        name='ds_report_summary', run_without_submitting=True)

    ds_report_about = pe.Node(
        DerivativesDataSink(base_directory=reportlets_dir,
                            desc='about', keep_dtype=True),
        name='ds_report_about', run_without_submitting=True)

    # Preprocessing of T1w (includes registration to MNI)

    workflow.connect([
        (bidssrc, bids_info, [('bold', 'in_file')]),
        (inputnode, summary, [('subjects_dir', 'subjects_dir')]),
        (bidssrc, summary, [('t1w', 't1w'),
                            ('t2w', 't2w'),
                            ('bold', 'bold')]),
        (bids_info, summary, [('subject', 'subject_id')]),
        (bidssrc, ds_report_summary, [('bold', 'source_file')]),
        (summary, ds_report_summary, [('out_report', 'in_file')]),
        (bidssrc, ds_report_about, [('bold', 'source_file')]),
        (about, ds_report_about, [('out_report', 'in_file')]),
    ])

    # Overwrite ``out_path_base`` of smriprep's DataSinks
    for node in workflow.list_node_names():
        if node.split('.')[-1].startswith('ds_'):
            workflow.get_node(node).interface.out_path_base = 'uchicagoABCDProcessing'

    for i in range(len(subject_data['bold'])):
        dfa_wf = init_dfa_workflow(
            bold=subject_data['bold'][i],
            brainmask=subject_data['mask'][i] if subject_data.keys().__contains__('mask') else None,
            csv=subject_data['csv'][i] if subject_data.keys().__contains__('csv') else None,
            min_freq=opts.minimum_frequency,
            max_freq=opts.maximum_frequency,
            drop_vols=opts.skip_vols
        )
        workflow.connect([
            (inputnode, dfa_wf, [('subjects_dir', 'inputnode.subjects_dir')]),
        ])

        outputs_wf = init_datasink_wf(bids_root=str(layout.root), output_dir=str(opts.output_dir))

        workflow.connect([(dfa_wf, outputs_wf, [('inputnode.bold', 'inputnode.source_file')])])
        # outputs_wf.inputs.source_file = subject_data['csv'][i] if subject_data.__contains__('mask') else subject_data['bold'][i]

        workflow.connect([(dfa_wf, outputs_wf, [
            ('outputnode.hurst', 'inputnode.dfa_h'),
            ('outputnode.rsquared', 'inputnode.dfa_ci'),
            ('outputnode.confidence_intervals', 'inputnode.dfa_rsquared')
        ])])

    return workflow


def _prefix(subid):
    if subid.startswith('sub-'):
        return subid
    return '-'.join(('sub', subid))


def _pop(inlist):
    if isinstance(inlist, (list, tuple)):
        return inlist[0]
    return inlist
