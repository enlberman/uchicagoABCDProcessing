import re
from collections import OrderedDict
from multiprocessing import cpu_count
from pathlib import Path

from bids import BIDSLayout


def build_workflow(opts, retval):
    from nipype import logging as nlogging, config as ncfg
    from niworkflows.utils.bids import collect_participants
    from niworkflows.reports import generate_reports
    from ..__about__ import __version__
    from time import strftime
    import uuid
    from ..workflows.base import init_base_wf

    build_log = nlogging.getLogger('nipype.workflow')

    INIT_MSG = """
    #     Running uchicagoABCDProcessing version {version}:
    #       * BIDS dataset path: {bids_dir}.
    #       * Participant list: {subject_list}.
    #       * Run identifier: {uuid}.
    #     """.format

    output_spaces = opts.output_spaces or OrderedDict([('MNI152NLin2009cAsym', {})])

    bids_dir = opts.bids_dir.resolve()
    output_dir = opts.output_dir.resolve()
    work_dir = opts.work_dir.resolve()

    retval['return_code'] = 1
    retval['workflow'] = None
    retval['bids_dir'] = str(bids_dir)
    retval['output_dir'] = str(output_dir)
    retval['work_dir'] = str(work_dir)

    if output_dir == bids_dir:
        build_log.error(
            'The selected output folder is the same as the input BIDS folder. '
            'Please modify the output path (suggestion: %s).',
            bids_dir / 'derivatives' / ('uchicagoABCDProcessing-%s' % __version__.split('+')[0]))
        retval['return_code'] = 1
        return retval

    # Set up some instrumental utilities
    run_uuid = '%s_%s' % (strftime('%Y%m%d-%H%M%S'), uuid.uuid4())
    retval['run_uuid'] = run_uuid

    # First check that bids_dir looks like a BIDS folder
    layout = BIDSLayout(str(bids_dir), validate=False,
                        ignore=("code", "stimuli", "sourcedata", "models",
                                "derivatives", re.compile(r'^\.')))
    subject_list = collect_participants(
        layout, participant_label=opts.participant_label)
    retval['subject_list'] = subject_list

    # Load base plugin_settings from file if --use-plugin
    if opts.use_plugin is not None:
        from yaml import load as loadyml
        with open(opts.use_plugin) as f:
            plugin_settings = loadyml(f)
        plugin_settings.setdefault('plugin_args', {})
    else:
        # Defaults
        plugin_settings = {
            'plugin': 'MultiProc',
            'plugin_args': {
                'raise_insufficient': False,
                'maxtasksperchild': 1,
            }
        }

    # Resource management options
    # Note that we're making strong assumptions about valid plugin args
    # This may need to be revisited if people try to use batch plugins
    nthreads = plugin_settings['plugin_args'].get('n_procs')
    # Permit overriding plugin config with specific CLI options
    if nthreads is None or opts.nthreads is not None:
        nthreads = opts.nthreads
        if nthreads is None or nthreads < 1:
            nthreads = cpu_count()
        plugin_settings['plugin_args']['n_procs'] = nthreads

    if opts.mem_mb:
        plugin_settings['plugin_args']['memory_gb'] = opts.mem_mb / 1024

    omp_nthreads = opts.omp_nthreads
    if omp_nthreads == 0:
        omp_nthreads = min(nthreads - 1 if nthreads > 1 else cpu_count(), 8)

    if 1 < nthreads < omp_nthreads:
        build_log.warning(
            'Per-process threads (--omp-nthreads=%d) exceed total '
            'threads (--nthreads/--n_cpus=%d)', omp_nthreads, nthreads)
    retval['plugin_settings'] = plugin_settings

    # Set up directories
    log_dir = output_dir / 'uchicagoABCDProcessing' / 'logs'
    # Check and create output and working directories
    output_dir.mkdir(exist_ok=True, parents=True)
    log_dir.mkdir(exist_ok=True, parents=True)
    work_dir.mkdir(exist_ok=True, parents=True)

    # Nipype config (logs and execution)
    ncfg.update_config({
        'logging': {
            'log_directory': str(log_dir),
            'log_to_file': True
        },
        'execution': {
            'crashdump_dir': str(log_dir),
            'crashfile_format': 'txt',
            'get_linked_libs': False,
            'stop_on_first_crash': opts.stop_on_first_crash,
        },
        'monitoring': {
            'enabled': opts.resource_monitor,
            'sample_frequency': '0.5',
            'summary_append': True,
        }
    })

    if opts.resource_monitor:
        ncfg.enable_resource_monitor()

    # Called with reports only
    if opts.reports_only:
        build_log.log(25, 'Running --reports-only on participants %s', ', '.join(subject_list))
        if opts.run_uuid is not None:
            run_uuid = opts.run_uuid
            retval['run_uuid'] = run_uuid
        retval['return_code'] = generate_reports(
            subject_list, output_dir, work_dir, run_uuid,
            packagename='uchicagoABCDProcessing')
        return retval

    # Build main workflow
    build_log.log(25, INIT_MSG(
        version=__version__,
        bids_dir=bids_dir,
        subject_list=subject_list,
        uuid=run_uuid)
                  )

    retval['workflow'] = retval['workflow'] = init_base_wf(
        anat_only=opts.anat_only,
        aroma_melodic_dim=opts.aroma_melodic_dimensionality,
        bold2t1w_dof=opts.bold2t1w_dof,
        cifti_output=opts.cifti_output,
        debug=opts.sloppy,
        dummy_scans=opts.dummy_scans,
        echo_idx=opts.echo_idx,
        err_on_aroma_warn=opts.error_on_aroma_warnings,
        fmap_bspline=opts.fmap_bspline,
        fmap_demean=opts.fmap_no_demean,
        force_syn=opts.force_syn,
        freesurfer=opts.run_reconall,
        hires=opts.hires,
        ignore=opts.ignore,
        layout=layout,
        longitudinal=opts.longitudinal,
        low_mem=opts.low_mem,
        medial_surface_nan=opts.medial_surface_nan,
        omp_nthreads=omp_nthreads,
        output_dir=str(output_dir),
        output_spaces=output_spaces,
        run_uuid=run_uuid,
        regressors_all_comps=opts.return_all_components,
        regressors_fd_th=opts.fd_spike_threshold,
        regressors_dvars_th=opts.dvars_spike_threshold,
        skull_strip_fixed_seed=opts.skull_strip_fixed_seed,
        skull_strip_template=opts.skull_strip_template,
        subject_list=subject_list,
        t2s_coreg=opts.t2s_coreg,
        task_id=opts.task_id,
        use_aroma=opts.use_aroma,
        use_bbr=opts.use_bbr,
        use_syn=opts.use_syn_sdc,
        work_dir=str(work_dir),
        opts=opts
    )
    retval['return_code'] = 0

    logs_path = Path(output_dir) / 'uchicagoABCDProcessing' / 'logs'
    boilerplate = retval['workflow'].visit_desc()

    if boilerplate:
        citation_files = {
            ext: logs_path / ('CITATION.%s' % ext)
            for ext in ('bib', 'tex', 'md', 'html')
        }
        # To please git-annex users and also to guarantee consistency
        # among different renderings of the same file, first remove any
        # existing one
        for citation_file in citation_files.values():
            try:
                citation_file.unlink()
            except FileNotFoundError:
                pass

        citation_files['md'].write_text(boilerplate)
        build_log.log(25, 'Works derived from this uchicagoABCDProcessing execution should '
                          'include the following boilerplate:\n\n%s', boilerplate)
    return retval
