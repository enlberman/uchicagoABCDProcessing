from argparse import ArgumentParser

import numpy
from bids import BIDSLayout
from nipype import Workflow
import sys
import os
from copy import deepcopy

from nipype import __version__ as nipype_ver
from fmriprep.workflows.base import init_fmriprep_wf
from nipype.pipeline import engine as pe
from nipype.interfaces import utility as niu

from niworkflows.engine.workflows import LiterateWorkflow as Workflow
from niworkflows.interfaces.bids import (
    BIDSInfo
)

from uchicagoABCDProcessing.workflows.datasink import DEFAULT_MEMORY_MIN_GB
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
    # todo change this workflow and take out head motion correction from bold
    # todo pass the file locations from fmriprep (bold in mni,regressors, brainmask)


    # func workflows have the name like 'func_preproc_*_wf'
    #* :py:func:`~fmriprep.workflows.bold.hmc.init_bold_hmc_wf`
    #init_bold_hmc_wf(name='bold_hmc_wf',

    # list of func preproc workflows
    all_func_workflows = list(filter(lambda node_name: node_name.__contains__("func_preproc"), fmriprep_workflow.list_node_names()))
    unique_ses_task_func_workflows = numpy.unique(list(map(lambda wf_name: wf_name.split('.')[1], all_func_workflows)))
    workflow_base_name = all_func_workflows[0].split('.')[0]  ## making a big assumption that this is only being run for one subject at a time

    for unique_workflow in unique_ses_task_func_workflows:
        print(unique_workflow)
        wf = fmriprep_workflow.get_node(workflow_base_name + '.' + unique_workflow)
        bold_reference_wf = fmriprep_workflow.get_node(workflow_base_name + '.' + unique_workflow + '.' + 'bold_reference_wf')
        bold_hmc_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_hmc_wf')
        bold_confounds_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_confounds_wf')
        bold_bold_trans_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_bold_trans_wf')

        bold_t1_trans_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_t1_trans_wf')
        bold_t1_trans_wf_merge_xforms_node = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_t1_trans_wf' + '.' + 'merge_xforms')
        bold_t1_trans_wf_bold_to_t1w_transform_node = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_t1_trans_wf' + '.' + 'bold_to_t1w_transform')
        bold_t1_trans_wf_inputnode = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_t1_trans_wf' + '.' + 'inputnode')

        bold_std_trans_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_std_trans_wf')
        bold_std_trans_wf_merge_xforms_node = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_std_trans_wf' + '.' + 'merge_xforms')
        bold_std_trans_wf_bold_to_std_transform_node = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_std_trans_wf' + '.' + 'bold_to_std_transform')
        bold_std_trans_wf_inputnode = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_std_trans_wf' + '.' + 'inputnode')
        bold_std_trans_wf_select_std = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_std_trans_wf' + '.' + 'select_std')

        wf.disconnect([
            (bold_reference_wf, bold_hmc_wf, [
                ('outputnode.raw_ref_image', 'inputnode.raw_ref_image'),
                ('outputnode.bold_file', 'inputnode.bold_file'),
            ]),
            (bold_hmc_wf, bold_t1_trans_wf, [('outputnode.xforms', 'inputnode.hmc_xforms')]),
            (bold_hmc_wf, bold_confounds_wf, [('outputnode.movpar_file', 'inputnode.movpar_file')]),
            (bold_hmc_wf, bold_bold_trans_wf, [('outputnode.xforms', 'inputnode.hmc_xforms')]),
            (bold_hmc_wf, bold_std_trans_wf, [('outputnode.xforms', 'inputnode.hmc_xforms')]),
        ])
        bold_t1_trans_wf.disconnect([
            (bold_t1_trans_wf_inputnode, bold_t1_trans_wf_merge_xforms_node, [
                ('hmc_xforms', 'in2'),
                ('itk_bold_to_t1', 'in1')]),
            (bold_t1_trans_wf_merge_xforms_node, bold_t1_trans_wf_bold_to_t1w_transform_node, [('out', 'transforms')]),
        ])

        bold_std_trans_wf.disconnect([
            (bold_std_trans_wf_inputnode, bold_std_trans_wf_merge_xforms_node, [
                ('hmc_xforms', 'in3'),
                (('itk_bold_to_t1', _aslist), 'in2'),
                ]),
            (bold_std_trans_wf_select_std, bold_std_trans_wf_merge_xforms_node, [('anat2std_xfm', 'in1')]),
            (bold_std_trans_wf_merge_xforms_node, bold_std_trans_wf_bold_to_std_transform_node, [('out', 'transforms')]),
        ])

        merge_xforms_new = pe.Node(niu.Merge(1), name='merge_xforms_new',
                               run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)
        bold_t1_trans_wf.connect([
            (bold_t1_trans_wf_inputnode, merge_xforms_new, [('itk_bold_to_t1', 'in1')]),
            (merge_xforms_new, bold_t1_trans_wf_bold_to_t1w_transform_node, [('out', 'transforms')]),
        ])

        merge_xforms_new_std = pe.Node(niu.Merge(2), name='merge_xforms_new',
                                   run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)
        bold_std_trans_wf.connect([
            (bold_std_trans_wf_inputnode, merge_xforms_new_std, [
                (('itk_bold_to_t1', _aslist), 'in2'),
            ]),
            (bold_std_trans_wf_select_std, merge_xforms_new_std, [('anat2std_xfm', 'in1')]),
            (
            merge_xforms_new_std, bold_std_trans_wf_bold_to_std_transform_node, [('out', 'transforms')]),
        ])
        # for each workflow in the list find the subworkflow named bold_hmc_wf and then disconnect it and do the passthrough connections that we need
        """
        ****INPUTS****
        (bold_reference_wf, bold_hmc_wf, [
                ('outputnode.raw_ref_image', 'inputnode.raw_ref_image'),                    ***************** disconnect
                ('outputnode.bold_file', 'inputnode.bold_file')]),                          ***************** disconnect
        ****OUTPUTS****
        (bold_hmc_wf, bold_t1_trans_wf, [('outputnode.xforms', 'inputnode.hmc_xforms')]),   ***************** disconnect
        (bold_hmc_wf, bold_confounds_wf, [
                ('outputnode.movpar_file', 'inputnode.movpar_file')]),                      ***************** need to generate this
                ********************************************************************************************************
                movpar_file
                MCFLIRT motion parameters, normalized to SPM format (X, Y, Z, Rx, Ry, Rz)
                **********************************************************************************************************
        (bold_hmc_wf, bold_bold_trans_wf, [
                ('outputnode.xforms', 'inputnode.hmc_xforms')]),                            ***************** disconnect
        ***********************************************************************
        in the bold_t1_trans_wf we need to replace the inclusion of hmc xforms init_bold_t1_trans_wf(name='bold_t1_trans_wf',
        merge_xforms = pe.Node(niu.Merge(1), name='merge_xforms',
                                   run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)
        ****HOW TO CONNECT**********
        workflow.connect([
                # merge transforms
                (inputnode, merge_xforms, [
                    ('hmc_xforms', 'in%d' % nforms),
                    ('itk_bold_to_t1', 'in1')]),
                (merge_xforms, bold_to_t1w_transform, [('out', 'transforms')]),
        ************************************************************************
        
        """
        bold_sdc_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'sdc_bypass_wf')
        bold_split_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'bold_split_wf')
        carpetplot_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'carpetplot_wf')
        inputnode = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'inputnode')

        wf.disconnect([
            (bold_sdc_wf, bold_bold_trans_wf, [
                ('outputnode.out_warp', 'inputnode.fieldwarp'),
             ('outputnode.bold_mask', 'inputnode.bold_mask')]
             ),
            (bold_split_wf, bold_bold_trans_wf, [
                ('out_files', 'inputnode.bold_file')]
            ),
            # (bold_hmc_wf, bold_bold_trans_wf, [
            #     ('outputnode.xforms', 'inputnode.hmc_xforms')]
            #  ),
            (bold_bold_trans_wf, bold_confounds_wf, [
                ('outputnode.bold', 'inputnode.bold'),
                ('outputnode.bold_mask', 'inputnode.bold_mask')]
             ),
            (bold_bold_trans_wf, bold_std_trans_wf, [('outputnode.bold_mask','inputnode.bold_mask')]),
            (bold_bold_trans_wf, carpetplot_wf, [
                ('outputnode.bold', 'inputnode.bold'),
                ('outputnode.bold_mask', 'inputnode.bold_mask')]
            ),
        ])

        wf.connect([
            (inputnode, bold_confounds_wf, [('bold_file', 'inputnode.bold')]),
            (bold_sdc_wf, bold_confounds_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')]),
            (bold_sdc_wf, bold_std_trans_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')]),
            (inputnode, carpetplot_wf, [('bold_file', 'inputnode.bold')]),
            (bold_sdc_wf, carpetplot_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')])
        ])
        ##we dont nee the bold bold trans wf
        """
        bold_bold_trans_wf = init_bold_preproc_trans_wf(
            mem_gb=mem_gb['resampled'],
            omp_nthreads=omp_nthreads,
            use_compression=not low_mem,
            use_fieldwarp=(fmaps is not None or use_syn),
            name='bold_bold_trans_wf'
        )
                    **Outputs**            
                        bold
                            BOLD series, resampled in native space, including all preprocessing
                        bold_mask
                            BOLD series mask calculated with the new time-series
                        bold_ref
                            BOLD reference image: an average-like 3D image of the time-series
                        bold_ref_brain
                            Same as ``bold_ref``, but once the brain mask has been applied
        *****INPUTS****
        (bold_sdc_wf, bold_bold_trans_wf, [
                ('outputnode.out_warp', 'inputnode.fieldwarp'),             ***************** disconnect
                ('outputnode.bold_mask', 'inputnode.bold_mask')]),          ***************** disconnect
        (bold_split, bold_bold_trans_wf, [
                ('out_files', 'inputnode.bold_file')]),                     ***************** disconnect
        (bold_hmc_wf, bold_bold_trans_wf, [
                ('outputnode.xforms', 'inputnode.hmc_xforms')]),            ***************** disconnect
        ****OUTPUTS****       
        (bold_bold_trans_wf, bold_confounds_wf, [
                    ('outputnode.bold', 'inputnode.bold'),                  ****************** replace with (inputnode, bold_confounds_wf, [('bold_file', 'inputnode.bold')])
                    ('outputnode.bold_mask', 'inputnode.bold_mask')]),      ****************** replace with (bold_sdc_wf, bold_confounds_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')])
        (bold_bold_trans_wf if not multiecho else bold_t2s_wf, bold_std_trans_wf, [     
                    ('outputnode.bold_mask', 'inputnode.bold_mask')]),      ****************** replace with (bold_sdc_wf, bold_std_trans_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')])
        (bold_bold_trans_wf if not multiecho else bold_t2s_wf, carpetplot_wf, [
                        ('outputnode.bold', 'inputnode.bold'),              ****************** replace with (inputnode, carpetplot_wf, [('bold_file', 'inputnode.bold')])
                        ('outputnode.bold_mask', 'inputnode.bold_mask')]),  ****************** replace with (bold_sdc_wf, carpetplot_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')])
        """

        print()
    # connect outputs to new pieces of confound regression hurst,matrices, and parcellation
    """
    (bold_std_trans_wf, func_derivatives_wf, [
                ('poutputnode.templates', 'inputnode.template'),
                ('poutputnode.bold_std_ref', 'inputnode.bold_std_ref'),
                ('poutputnode.bold_std', 'inputnode.bold_std'),
                ('poutputnode.bold_mask_std', 'inputnode.bold_mask_std'),
            ]),
    bold_std_trans_wf = init_bold_std_trans_wf(
            name='bold_std_trans_wf',
    """



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

def _aslist(in_value):
    if isinstance(in_value, list):
        return in_value
    return [in_value]
