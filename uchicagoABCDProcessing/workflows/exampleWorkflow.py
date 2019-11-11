from nipype.pipeline import engine as pe
from nipype.interfaces import (
    utility as niu,

)
from niworkflows.engine.workflows import LiterateWorkflow as Workflow
from ..interfaces import DFA, DerivativesDataSink


def init_dfa_workflow(bold, brainmask, csv, min_freq=0.01, max_freq=0.1, drop_vols=5, name='dfa_wf'):#bids_root, omp_nthreads, output_dir, output_format, reportlets_dir, debug=False, name='dfa_wf'):

    workflow = Workflow(name=name)
    desc = """Fractal Scaling via Detrended Fluctuation Analysis
    : """
    # desc += """\
    # A total of {num_t1w} T1-weighted (T1w) images were found within the input
    # BIDS dataset.
    # All of them were corrected for intensity non-uniformity (INU)
    # """ if num_t1w > 1 else """\
    # The T1-weighted (T1w) image was corrected for intensity non-uniformity (INU)
    # """
    # desc += """\
    # with `N4BiasFieldCorrection` [@n4], distributed with ANTs {ants_ver} \
    # [@ants, RRID:SCR_004757]"""
    # desc += '.\n' if num_t1w > 1 else ", and used as T1w-reference throughout the workflow.\n"
    #
    # desc += """\
    # The T1w-reference was then skull-stripped with a *Nipype* implementation of
    # the `antsBrainExtraction.sh` workflow (from ANTs), using {skullstrip_tpl}
    # as target template.
    # Brain tissue segmentation of cerebrospinal fluid (CSF),
    # white-matter (WM) and gray-matter (GM) was performed on
    # the brain-extracted T1w using `fast` [FSL {fsl_ver}, RRID:SCR_002823,
    # @fsl_fast].
    # """
    #
    # workflow.__desc__ = desc.format(
    #     ants_ver=ANTsInfo.version() or '(version unknown)',
    #     fsl_ver=fsl.FAST().version or '(version unknown)',
    #     num_t1w=num_t1w,
    #     skullstrip_tpl=skull_strip_template[0],
    # )

    inputnode = pe.Node(
        niu.IdentityInterface(fields=['bold','csv', 'max_frequency', 'min_frequency', 'drop_vols', 'brainmask', 'subjects_dir']),
        name='inputnode')

    inputnode.inputs.bold = bold
    inputnode.inputs.csv = csv
    inputnode.inputs.max_frequency = max_freq
    inputnode.inputs.min_frequency = min_freq
    inputnode.inputs.brainmask = brainmask
    inputnode.inputs.drop_vols = drop_vols

    outputnode = pe.Node(niu.IdentityInterface(
        fields=['rsquared', 'confidence_intervals', 'hurst']),
        name='outputnode')

    dfanode = pe.Node(DFA(), name='dfa')

    workflow.connect([
        (inputnode, dfanode, [('bold', 'bold')]),
        (inputnode, dfanode, [('max_frequency', 'max_frequency')]),
        (inputnode, dfanode, [('min_frequency', 'min_frequency')]),
        (inputnode, dfanode, [('drop_vols', 'drop_vols')]),
        # (dfanode, outputnode, [('out_report', 'out_report')]),
        (dfanode, outputnode, [('hurst', 'hurst')]),
        (dfanode, outputnode, [('confidence_intervals', 'confidence_intervals')]),
        (dfanode, outputnode, [('rsquared', 'rsquared')]),
    ])

    if csv is not None:
        workflow.connect([
            (inputnode, dfanode, [('csv', 'csv')]),
        ])
    if brainmask is not None:
        workflow.connect([
            (inputnode, dfanode, [('brainmask', 'brainmask')]),
        ])








    # ds_report_summary = pe.Node(
    #     DerivativesDataSink(desc='summary', keep_dtype=True),
    #     name='ds_report_summary', run_without_submitting=True,
    #     mem_gb=1)

    # summary = pe.Node(
    #     FunctionalSummary(
    #         slice_timing=run_stc,
    #         registration=('FSL', 'FreeSurfer')[freesurfer],
    #         registration_dof=bold2t1w_dof,
    #         pe_direction=metadata.get("PhaseEncodingDirection"),
    #         tr=metadata.get("RepetitionTime")),
    #     name='summary', mem_gb=DEFAULT_MEMORY_MIN_GB, run_without_submitting=True)
    # summary.inputs.dummy_scans = dummy_scans

    # workflow.connect([
    #     (summary, ds_report_summary, [('out_report', 'in_file')]),
    #     (bold_reference_wf, ds_report_validation, [
    #         ('outputnode.validation_report', 'in_file')]),
    # ])
    return workflow