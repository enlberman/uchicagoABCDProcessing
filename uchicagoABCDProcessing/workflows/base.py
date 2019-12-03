from argparse import ArgumentParser

import numpy
from atlasTransform.interfaces import AtlasTransform
from enlNipypeInterfaces.interfaces import FisherRToZMatrix
from fMRIConfoundRegression.interfaces import ThirtySixParameter
from neuroHurst.interfaces import DFA

from fmriprep.workflows.base import init_fmriprep_wf
from nipype.pipeline import engine as pe
from nipype.interfaces import utility as niu, afni

from uchicagoABCDProcessing.interfaces import Motion
from uchicagoABCDProcessing.workflows.datasink import DEFAULT_MEMORY_MIN_GB


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

    """
    make fake dir
    """


    # list of func preproc workflows
    all_func_workflows = list(filter(lambda node_name: node_name.__contains__("func_preproc"), fmriprep_workflow.list_node_names()))
    unique_ses_task_func_workflows = numpy.unique(list(map(lambda wf_name: wf_name.split('.')[1], all_func_workflows)))
    workflow_base_name = all_func_workflows[0].split('.')[0]  ## making a big assumption that this is only being run for one subject at a time

    for unique_workflow in unique_ses_task_func_workflows:
        # collect all the nodes that we need to disconnect hmc (head motion correction) and bold_bold_trans (bold realignment) workflows
        wf = fmriprep_workflow.get_node(workflow_base_name + '.' + unique_workflow)
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

        wf.remove_nodes([bold_hmc_wf])  # disconnect hmc workflow

        # disconnect merge xform nodes we replace them below
        bold_std_trans_wf.remove_nodes([bold_std_trans_wf_merge_xforms_node])
        bold_t1_trans_wf.remove_nodes([bold_t1_trans_wf_merge_xforms_node])

        merge_xforms_new = pe.Node(niu.Merge(1), name='merge_xforms_new',
                               run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)
        bold_t1_trans_wf.connect([
            (bold_t1_trans_wf_inputnode, merge_xforms_new, [('itk_bold_to_t1', 'in1')]),
            (merge_xforms_new, bold_t1_trans_wf_bold_to_t1w_transform_node, [('out', 'transforms')]),
        ])

        merge_xforms_new_std = pe.Node(niu.Merge(3), name='merge_xforms_new',
                                   run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)
        bold_std_trans_wf.connect([
            (bold_std_trans_wf_inputnode, merge_xforms_new_std, [
                ('fieldwarp', 'in3'),
                (('itk_bold_to_t1', _aslist), 'in2'),
            ]),
            (bold_std_trans_wf_select_std, merge_xforms_new_std, [('anat2std_xfm', 'in1')]),
            (
            merge_xforms_new_std, bold_std_trans_wf_bold_to_std_transform_node, [('out', 'transforms')]),
        ])

        # collect some more nodes necessary for removing the bold bold trans (realignment) workflow
        bold_sdc_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'sdc_bypass_wf')
        carpetplot_wf = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'carpetplot_wf')
        inputnode = fmriprep_workflow.get_node(
            workflow_base_name + '.' + unique_workflow + '.' + 'inputnode')

        # remove the realignment workflow
        wf.remove_nodes([bold_bold_trans_wf])

        # rewire the workflow after removing the bold bold trans and hmc workflows
        wf.connect([
            (inputnode, bold_confounds_wf, [('bold_file', 'inputnode.bold')]),
            (bold_sdc_wf, bold_confounds_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')]),
            (bold_sdc_wf, bold_std_trans_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')]),
            (inputnode, carpetplot_wf, [('bold_file', 'inputnode.bold')]),
            (bold_sdc_wf, carpetplot_wf, [('outputnode.bold_mask', 'inputnode.bold_mask')])
        ])

        motionNode = pe.Node(Motion(),name='motion_file', run_without_submitting=True,mem_gb=DEFAULT_MEMORY_MIN_GB)

        wf.connect([
            (inputnode, motionNode, [('bold_file', 'bold')]),
            (motionNode, bold_confounds_wf, [('out', 'inputnode.movpar_file')])
        ])


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
        # add confound regression
        confoundRegressionNode = pe.Node(ThirtySixParameter(), name='36p') #output is regressed

        wf.connect([
            (bold_std_trans_wf, confoundRegressionNode, [('outputnode.bold_std', 'bold')]),
            (bold_std_trans_wf, confoundRegressionNode, [('outputnode.bold_mask_std', 'brainmask')]),
            (bold_confounds_wf, confoundRegressionNode, [('outputnode.confounds_file', 'regressors')]),
        ])

        # add despike
        despikeNode = pe.Node(afni.Despike(), name='despike') #output is out_file
        despikeNode.inputs.outputtype = 'NIFTI_GZ'
        despikeNode.inputs.args = '-NEW25'

        wf.connect([
            (confoundRegressionNode, despikeNode, [('regressed', 'in_file')]),
            ])

        # get a list of all the deconfounded outputs for final stages of processing below
        merge_deconfounded = pe.Node(niu.Merge(2), name='merge_deconfounded',
                                       run_without_submitting=True, mem_gb=DEFAULT_MEMORY_MIN_GB)

        wf.connect([
            (confoundRegressionNode, merge_deconfounded, [('regressed', 'in1')]),
            (despikeNode, merge_deconfounded, [('out_file', 'in2')])
        ])

        # do each parcellation and final processing seperately
        for parcellation in opts.parcellations:
            parcellation_name, number_of_clusters = parcellation.split('_') # e.g. parcellation='craddock_400'
            transformNode = pe.Node(AtlasTransform(), name='transform_%s' % parcellation) #output is transformed

            # setup the inputs for the parcellation node
            transformNode.inputs.atlas_name = parcellation_name
            transformNode.inputs.resolution = opts.resolution
            transformNode.inputs.number_of_clusters = int(number_of_clusters)
            transformNode.inputs.similarity_measure = opts.similarity_measure
            transformNode.inputs.algorithm = opts.algorithm
            transformNode.inputs.bids_dir = layout.root

            # connectup the parcellation node
            wf.connect([
                (merge_deconfounded, transformNode,[('out', 'nifti')])
            ])

            ## now connect parcellation output to hurst and connectivity
            connectivityNode = pe.Node(FisherRToZMatrix(), name='connectivity_%s' % parcellation, itersource=merge_deconfounded.name) #input is csv output is connectivity
            wf.connect([
                (transformNode, connectivityNode, [('transformed', 'csv')])
            ])

            hurstNode = pe.Node(DFA(), name='dfa_%s' % parcellation, itersource=merge_deconfounded.name)
            wf.connect([
                (transformNode, hurstNode, [('transformed', 'csv')]),
                (merge_deconfounded, hurstNode, [('out', 'bold')])
            ])

    #finally setup the download workflow and connect it up
    # bids_src = fmriprep_workflow.get_node(workflow_base_name + '.' + 'bidssrc')
    """
    need to connect
    bids_src.inpus.subject_data:
    bids_info = pe.Node(BIDSInfo(
        bids_dir=layout.root, bids_validate=False), name='bids_info')
        anat derivatives workflow raw_sources.inputs.bids_root = bids_root
    """
    print()
    return fmriprep_workflow


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
