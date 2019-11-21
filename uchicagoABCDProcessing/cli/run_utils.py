import glob
import os
from pathlib import Path
import logging
import sys
import gc
import warnings
from argparse import ArgumentParser
from argparse import ArgumentDefaultsHelpFormatter
from NDATools.Configuration import ClientConfiguration

import numpy

from uchicagoABCDProcessing.interfaces.oracle import OracleQuery

from uchicagoABCDProcessing.cli.version import check_latest, is_flagged


def _warn_redirect(message, category, filename, lineno, logger, file=None, line=None):
    logger.warning('Captured warning (%s): %s', category, message)


def check_deps(workflow):
    from nipype.utils.filemanip import which
    return sorted(
        (node.interface.__class__.__name__, node.interface._cmd)
        for node in workflow._get_all_nodes()
        if (hasattr(node.interface, '_cmd') and
            which(node.interface._cmd.split()[0]) is None))


def get_parser() -> ArgumentParser():
    """Build parser object"""
    from smriprep.cli.utils import ParseTemplates, output_space as _output_space
    from templateflow.api import templates
    from packaging.version import Version
    from ..__about__ import __version__
    from fmriprep.workflows.bold.resampling import  NONSTANDARD_REFERENCES
    # from fmriprep.cli.version import check_latest, is_flagged

    verstr = 'fmriprep v{}'.format(__version__)
    currentv = Version(__version__)
    is_release = not any((currentv.is_devrelease, currentv.is_prerelease, currentv.is_postrelease))

    parser = ArgumentParser(description='FMRIPREP: fMRI PREProcessing workflows',
                            formatter_class=ArgumentDefaultsHelpFormatter)

    # Arguments as specified by BIDS-Apps
    # required, positional arguments
    # IMPORTANT: they must go directly with the parser object
    parser.add_argument('bids_dir', action='store', type=Path,
                        help='the root folder of a BIDS valid dataset (sub-XXXXX folders should '
                             'be found at the top level in this folder).')
    parser.add_argument('output_dir', action='store', type=Path,
                        help='the output path for the outcomes of preprocessing and visual '
                             'reports')
    parser.add_argument('analysis_level', choices=['participant'],
                        help='processing stage to be run, only "participant" in the case of '
                             'FMRIPREP (see BIDS-Apps specification).')

    parser.add_argument('--parcellations', action='store', nargs="+", default=['shen_268'],
        choices=['shen_268', 'craddock_400', 'craddock_270'],
        help='which parcellations to use (a space delimited list)', required=True,)
    parser.add_argument('--resolution', action='store', type=int, default=1,choices=[1,2],
                        help='parcellation resolution', )
    parser.add_argument('--similarity_measure', action='store', type=str, default='t',choices=['t','s'],
                        help='craddock parcellation similarity_measure', )
    parser.add_argument('--algorithm', action='store', default='2level', choices=['2level', 'mean', None],
                        help='craddock parcellation algorithm', )

    parser.add_argument('--miNDAR_host', action='store', type=str,
                        help='miDNAR host')
    parser.add_argument('--miNDAR_password', action='store', type=str,
                        help='miDNAR password')
    parser.add_argument('--miNDAR_username', action='store', type=str,
                        help='miDNAR username')
    parser.add_argument('--nda_username', action='store', type=str,
                        help='NDA username')
    parser.add_argument('--nda_password', action='store', type=str,
                        help='nda password')

    parser.add_argument('--session', action='store', type=str, default='ses-baselineYear1Arm1',
                        choices=['ses-baselineYear1Arm1'],
                        help='which session')

    parser.add_argument('--skip_download', action='store', type=bool, default=False,
                        help='skip downloading subject data')

    # optional arguments
    parser.add_argument('--version', action='version', version=verstr)

    g_bids = parser.add_argument_group('Options for filtering BIDS queries')
    g_bids.add_argument('--skip_bids_validation', '--skip-bids-validation', action='store_true',
                        default=False,
                        help='assume the input dataset is BIDS compliant and skip the validation')
    g_bids.add_argument('--participant_label', '--participant-label', action='store', nargs='+',
                        help='a space delimited list of participant identifiers or a single '
                             'identifier (the sub- prefix can be removed)')
    # Re-enable when option is actually implemented
    # g_bids.add_argument('-s', '--session-id', action='store', default='single_session',
    #                     help='select a specific session to be processed')
    # Re-enable when option is actually implemented
    # g_bids.add_argument('-r', '--run-id', action='store', default='single_run',
    #                     help='select a specific run to be processed')
    g_bids.add_argument('-t', '--task-id', action='store',
                        help='select a specific task to be processed')
    g_bids.add_argument('--echo-idx', action='store', type=int,
                        help='select a specific echo to be processed in a multiecho series')

    g_perfm = parser.add_argument_group('Options to handle performance')
    g_perfm.add_argument('--nthreads', '--n_cpus', '-n-cpus', action='store', type=int,
                         help='maximum number of threads across all processes')
    g_perfm.add_argument('--omp-nthreads', action='store', type=int, default=0,
                         help='maximum number of threads per-process')
    g_perfm.add_argument('--mem_mb', '--mem-mb', action='store', default=0, type=int,
                         help='upper bound memory limit for FMRIPREP processes')
    g_perfm.add_argument('--low-mem', action='store_true',
                         help='attempt to reduce memory usage (will increase disk usage '
                              'in working directory)')
    g_perfm.add_argument('--use-plugin', action='store', default=None,
                         help='nipype plugin configuration file')
    g_perfm.add_argument('--anat-only', action='store_true',
                         help='run anatomical workflows only')
    g_perfm.add_argument('--boilerplate', action='store_true',
                         help='generate boilerplate only')
    g_perfm.add_argument('--ignore-aroma-denoising-errors', action='store_true',
                         default=False,
                         help='DEPRECATED (now does nothing, see --error-on-aroma-warnings) '
                              '- ignores the errors ICA_AROMA returns when there are no '
                              'components classified as either noise or signal')
    g_perfm.add_argument('--error-on-aroma-warnings', action='store_true',
                         default=False,
                         help='Raise an error if ICA_AROMA does not produce sensible output '
                              '(e.g., if all the components are classified as signal or noise)')
    g_perfm.add_argument("-v", "--verbose", dest="verbose_count", action="count", default=0,
                         help="increases log verbosity for each occurence, debug level is -vvv")
    g_perfm.add_argument('--debug', action='store_true', default=False,
                         help='DEPRECATED - Does not do what you want.')

    g_conf = parser.add_argument_group('Workflow configuration')
    g_conf.add_argument(
        '--ignore', required=False, action='store', nargs="+", default=[],
        choices=['fieldmaps', 'slicetiming', 'sbref'],
        help='ignore selected aspects of the input dataset to disable corresponding '
             'parts of the workflow (a space delimited list)')
    g_conf.add_argument(
        '--longitudinal', action='store_true',
        help='treat dataset as longitudinal - may increase runtime')
    g_conf.add_argument(
        '--t2s-coreg', action='store_true',
        help='If provided with multi-echo BOLD dataset, create T2*-map and perform '
             'T2*-driven coregistration. When multi-echo data is provided and this '
             'option is not enabled, standard EPI-T1 coregistration is performed '
             'using the middle echo.')
    g_conf.add_argument(
        '--output-spaces', nargs='+', action=ParseTemplates,
        help="""\
Standard and non-standard spaces to resample anatomical and functional images to. \
Standard spaces may be specified by the form \
``<TEMPLATE>[:res-<resolution>][:cohort-<label>][...]``, where ``<TEMPLATE>`` is \
a keyword (valid keywords: %s) or path pointing to a user-supplied template, and \
may be followed by optional, colon-separated parameters. \
Non-standard spaces (valid keywords: %s) imply specific orientations and sampling \
grids. \
Important to note, the ``res-*`` modifier does not define the resolution used for \
the spatial normalization.
For further details, please check out \
https://fmriprep.readthedocs.io/en/%s/spaces.html""" % (
            ', '.join('"%s"' % s for s in templates()), ', '.join(NONSTANDARD_REFERENCES),
            currentv.base_version if is_release else 'latest'))

    g_conf.add_argument(
        '--output-space', required=False, action='store', type=str, nargs='+',
        choices=['T1w', 'template', 'fsnative', 'fsaverage', 'fsaverage6', 'fsaverage5'],
        help='DEPRECATED: please use ``--output-spaces`` instead.'
    )
    g_conf.add_argument(
        '--template', required=False, action='store', type=str,
        choices=['MNI152NLin2009cAsym'],
        help='volume template space (default: MNI152NLin2009cAsym). '
             'DEPRECATED: please use ``--output-spaces`` instead.')
    g_conf.add_argument(
        '--template-resampling-grid', required=False, action='store',
        help='Keyword ("native", "1mm", or "2mm") or path to an existing file. '
             'Allows to define a reference grid for the resampling of BOLD images in template '
             'space. Keyword "native" will use the original BOLD grid as reference. '
             'Keywords "1mm" and "2mm" will use the corresponding isotropic template '
             'resolutions. If a path is given, the grid of that image will be used. '
             'It determines the field of view and resolution of the output images, '
             'but is not used in normalization. '
             'DEPRECATED: please use ``--output-spaces`` instead.')
    g_conf.add_argument('--bold2t1w-dof', action='store', default=6, choices=[6, 9, 12], type=int,
                        help='Degrees of freedom when registering BOLD to T1w images. '
                             '6 degrees (rotation and translation) are used by default.')
    g_conf.add_argument(
        '--force-bbr', action='store_true', dest='use_bbr', default=None,
        help='Always use boundary-based registration (no goodness-of-fit checks)')
    g_conf.add_argument(
        '--force-no-bbr', action='store_false', dest='use_bbr', default=None,
        help='Do not use boundary-based registration (no goodness-of-fit checks)')
    g_conf.add_argument(
        '--medial-surface-nan', required=False, action='store_true', default=False,
        help='Replace medial wall values with NaNs on functional GIFTI files. Only '
        'performed for GIFTI files mapped to a freesurfer subject (fsaverage or fsnative).')
    g_conf.add_argument(
        '--dummy-scans', required=False, action='store', default=None, type=int,
        help='Number of non steady state volumes.')

    # ICA_AROMA options
    g_aroma = parser.add_argument_group('Specific options for running ICA_AROMA')
    g_aroma.add_argument('--use-aroma', action='store_true', default=False,
                         help='add ICA_AROMA to your preprocessing stream')
    g_aroma.add_argument('--aroma-melodic-dimensionality', action='store',
                         default=-200, type=int,
                         help='Exact or maximum number of MELODIC components to estimate '
                         '(positive = exact, negative = maximum)')

    # Confounds options
    g_confounds = parser.add_argument_group('Specific options for estimating confounds')
    g_confounds.add_argument(
        '--return-all-components', required=False, action='store_true', default=False,
        help='Include all components estimated in CompCor decomposition in the confounds '
             'file instead of only the components sufficient to explain 50 percent of '
             'BOLD variance in each CompCor mask')
    g_confounds.add_argument(
        '--fd-spike-threshold', required=False, action='store', default=0.5, type=float,
        help='Threshold for flagging a frame as an outlier on the basis of framewise '
             'displacement')
    g_confounds.add_argument(
        '--dvars-spike-threshold', required=False, action='store', default=1.5, type=float,
        help='Threshold for flagging a frame as an outlier on the basis of standardised '
             'DVARS')

    #  ANTs options
    g_ants = parser.add_argument_group('Specific options for ANTs registrations')
    g_ants.add_argument(
        '--skull-strip-template', action='store', default='OASIS30ANTs', type=_output_space,
        help='select a template for skull-stripping with antsBrainExtraction')
    g_ants.add_argument('--skull-strip-fixed-seed', action='store_true',
                        help='do not use a random seed for skull-stripping - will ensure '
                             'run-to-run replicability when used with --omp-nthreads 1')

    # Fieldmap options
    g_fmap = parser.add_argument_group('Specific options for handling fieldmaps')
    g_fmap.add_argument('--fmap-bspline', action='store_true', default=False,
                        help='fit a B-Spline field using least-squares (experimental)')
    g_fmap.add_argument('--fmap-no-demean', action='store_false', default=True,
                        help='do not remove median (within mask) from fieldmap')

    # SyN-unwarp options
    g_syn = parser.add_argument_group('Specific options for SyN distortion correction')
    g_syn.add_argument('--use-syn-sdc', action='store_true', default=False,
                       help='EXPERIMENTAL: Use fieldmap-free distortion correction')
    g_syn.add_argument('--force-syn', action='store_true', default=False,
                       help='EXPERIMENTAL/TEMPORARY: Use SyN correction in addition to '
                       'fieldmap correction, if available')

    # FreeSurfer options
    g_fs = parser.add_argument_group('Specific options for FreeSurfer preprocessing')
    g_fs.add_argument(
        '--fs-license-file', metavar='PATH', type=Path,
        help='Path to FreeSurfer license key file. Get it (for free) by registering'
             ' at https://surfer.nmr.mgh.harvard.edu/registration.html')

    # Surface generation xor
    g_surfs = parser.add_argument_group('Surface preprocessing options')
    g_surfs.add_argument('--no-submm-recon', action='store_false', dest='hires',
                         help='disable sub-millimeter (hires) reconstruction')
    g_surfs_xor = g_surfs.add_mutually_exclusive_group()
    g_surfs_xor.add_argument('--cifti-output', action='store_true', default=False,
                             help='output BOLD files as CIFTI dtseries')
    g_surfs_xor.add_argument('--fs-no-reconall', '--no-freesurfer',
                             action='store_false', dest='run_reconall',
                             help='disable FreeSurfer surface preprocessing.'
                             ' Note : `--no-freesurfer` is deprecated and will be removed in 1.2.'
                             ' Use `--fs-no-reconall` instead.')

    g_other = parser.add_argument_group('Other options')
    g_other.add_argument('-w', '--work-dir', action='store', type=Path, default=Path('work'),
                         help='path where intermediate results should be stored')
    g_other.add_argument(
        '--resource-monitor', action='store_true', default=False,
        help='enable Nipype\'s resource monitoring to keep track of memory and CPU usage')
    g_other.add_argument(
        '--reports-only', action='store_true', default=False,
        help='only generate reports, don\'t run workflows. This will only rerun report '
             'aggregation, not reportlet generation for specific nodes.')
    g_other.add_argument(
        '--run-uuid', action='store', default=None,
        help='Specify UUID of previous run, to include error logs in report. '
             'No effect without --reports-only.')
    g_other.add_argument('--write-graph', action='store_true', default=False,
                         help='Write workflow graph.')
    g_other.add_argument('--stop-on-first-crash', action='store_true', default=False,
                         help='Force stopping on first crash, even if a work directory'
                              ' was specified.')
    g_other.add_argument('--notrack', action='store_true', default=False,
                         help='Opt-out of sending tracking information of this run to '
                              'the FMRIPREP developers. This information helps to '
                              'improve FMRIPREP and provides an indicator of real '
                              'world usage crucial for obtaining funding.')
    g_other.add_argument('--sloppy', action='store_true', default=False,
                         help='Use low-quality tools for speed - TESTING ONLY')

    latest = check_latest()
    if latest is not None and currentv < latest:
        print("""\
You are using fMRIPrep-%s, and a newer version of fMRIPrep is available: %s.
Please check out our documentation about how and when to upgrade:
https://fmriprep.readthedocs.io/en/latest/faq.html#upgrading""" % (
            __version__, latest), file=sys.stderr)

    _blist = is_flagged()
    if _blist[0]:
        _reason = _blist[1] or 'unknown'
        print("""\
WARNING: Version %s of fMRIPrep (current) has been FLAGGED
(reason: %s).
That means some severe flaw was found in it and we strongly
discourage its usage.""" % (__version__, _reason), file=sys.stderr)

    return parser


def get_workflow(logger):
    from nipype import logging as nlogging
    from multiprocessing import set_start_method, Process, Manager
    from ..utils.bids import validate_input_dir
    from .build_workflow import build_workflow
    if __name__ == 'main':
        set_start_method('forkserver')
    warnings.showwarning = _warn_redirect
    opts = get_parser().parse_args()

    exec_env = os.name

    # special variable set in the container
    # if os.getenv('IS_DOCKER_8395080871'):
    #     exec_env = 'singularity'
    #     cgroup = Path('/proc/1/cgroup')
    #     if cgroup.exists() and 'docker' in cgroup.read_text():
    #         exec_env = 'docker'
    #         if os.getenv('DOCKER_VERSION_8395080871'):
    #             exec_env = 'fmriprep-docker'

    sentry_sdk = None
    if not opts.notrack:
        import sentry_sdk
        from ..utils.sentry import sentry_setup
        sentry_setup(opts, exec_env)

    aws_token_info = os.popen(
        "bash $NDA_TOKEN_GEN_DIR/curl/generate_token.sh '%s' '%s' 'https://nda.nih.gov/DataManager/dataManager'"
        % (opts.nda_username, opts.nda_password)
    ).readlines()
    secret_key = aws_token_info[2].split(':')[1].strip()
    access_key = aws_token_info[1].split(':')[1].strip()
    session_token = aws_token_info[3].split(':')[1].strip()

    if not opts.skip_download:
        # get_files = OracleQuery()
        # get_files.inputs.username = opts.miNDAR_username
        # get_files.inputs.password = opts.miNDAR_password
        # get_files.inputs.host = opts.miNDAR_host
        # get_files.inputs.service = 'ORCL'
        # get_files.inputs.write_to_file=False
        #
        # get_files.inputs.query = "select column_name from USER_TAB_COLUMNS where table_name = 'FMRIRESULTS01'"
        # get_files.run()
        # columns = get_files._results['out'].values.flatten()
        # scan_type = numpy.argwhere(columns == 'SCAN_TYPE').flatten()[0]
        # file_link = numpy.argwhere(columns == 'DERIVED_FILES').flatten()[0]
        #
        # get_files.inputs.query = "select * from FMRIRESULTS01 where SUBJECTKEY = '%s'" % opts.participant_label[0]
        # get_files.run()
        # subject_files = get_files._results['out']
        #
        # anat_and_func_files =subject_files[(subject_files[scan_type] =='MR structural (T1)') | (subject_files[scan_type] =='fMRI')][file_link].values
        anat_and_func_files = ['s3://NDAR_Central_2/submission_19161/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-rsfMRI_20170414114436.tgz', 's3://NDAR_Central_2/submission_19161/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-rsfMRI_20170414120922.tgz', 's3://NDAR_Central_2/submission_19161/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-rsfMRI_20170414121456.tgz', 's3://NDAR_Central_2/submission_19161/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-rsfMRI_20170414113822.tgz', 's3://NDAR_Central_2/submission_19137/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-MID-fMRI_20170414123932.tgz', 's3://NDAR_Central_2/submission_19137/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-MID-fMRI_20170414123343.tgz', 's3://NDAR_Central_2/submission_19137/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-T1_20170414113634.tgz', 's3://NDAR_Central_3/submission_19178/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-SST-fMRI_20170414125453.tgz', 's3://NDAR_Central_3/submission_19178/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-nBack-fMRI_20170414122216.tgz', 's3://NDAR_Central_3/submission_19178/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-SST-fMRI_20170414124837.tgz', 's3://NDAR_Central_3/submission_19178/NDARINVRCE62M22_baselineYear1Arm1_ABCD-MPROC-nBack-fMRI_20170414122744.tgz']

        download_links = os.path.join(opts.work_dir,'alls3.txt')
        os.makedirs(Path(download_links).parent, exist_ok=True)
        with open(download_links,'w') as file:
            for link in anat_and_func_files:
                file.writelines(link+'\n')

        nda_config = ClientConfiguration(
            username=opts.nda_username,
            password=opts.nda_password,
            secret_key=secret_key,
            access_key=access_key,
            settings_file='clientscripts/config/settings.cfg'
        )
        nda_config.make_config()

        download_dir = os.path.join(opts.work_dir,'downloads')
        os.system("downloadcmd %s -t -d %s" % (download_links, download_dir)) # download all the files

        bids_dir = os.path.join(opts.work_dir,'bids')
        subject_dir = os.path.join(bids_dir, 'sub-%s' % opts.participant_label[0].replace('_',''))
        session_dir = os.path.join(subject_dir, opts.session)
        func_dir = os.path.join(session_dir,'func')
        anat_dir = os.path.join(session_dir, 'anat')

        os.makedirs(func_dir, exist_ok=True)
        os.makedirs(anat_dir, exist_ok=True)

        downloaded_files = glob.glob(os.path.join(download_dir,"*","*.tgz"))

        for download in downloaded_files:  # untar files
            os.system('tar zxvf %s -C %s' % (download, download_dir))

        downloaded_func_files = glob.glob(os.path.join(download_dir,
                                                       'sub-%s' % opts.participant_label[0].replace('_',''),
                                                       opts.session,"func","*")
                                          )
        downloaded_anat_files = glob.glob(os.path.join(download_dir,
                                                       'sub-%s' % opts.participant_label[0].replace('_',''),
                                                       opts.session,"anat","*")
                                          )

        for file in downloaded_func_files:
            os.system('mv %s %s' % (file, func_dir))

        for file in downloaded_anat_files:
            os.system('mv %s %s' % (file, anat_dir))
        opts.bids_dir = bids_dir #cant just do this need the layout object

    # Validate inputs
    if not opts.skip_bids_validation:
        print("Making sure the input data is BIDS compliant (warnings can be ignored in most "
              "cases).")
        validate_input_dir(exec_env, opts.bids_dir, opts.participant_label)

    # Retrieve logging level
    log_level = int(max(25 - 5 * opts.verbose_count, logging.DEBUG))
    # Set logging
    logger.setLevel(log_level)
    nlogging.getLogger('nipype.workflow').setLevel(log_level)
    nlogging.getLogger('nipype.interface').setLevel(log_level)
    nlogging.getLogger('nipype.utils').setLevel(log_level)

    # Call build_workflow(opts, retval)
    with Manager() as mgr:
        retval = mgr.dict()
        p = Process(target=build_workflow, args=(opts, retval))
        p.start()
        p.join()

        retcode = p.exitcode or retval.get('return_code', 0)

        bids_dir = Path(retval.get('bids_dir'))
        output_dir = Path(retval.get('output_dir'))
        work_dir = Path(retval.get('work_dir'))
        plugin_settings = retval.get('plugin_settings', None)
        subject_list = retval.get('subject_list', None)
        neuroHurst_wf = retval.get('workflow', None)
        run_uuid = retval.get('run_uuid', None)

    if opts.reports_only:
        sys.exit(int(retcode > 0))

    if opts.boilerplate:
        sys.exit(int(retcode > 0))

    if neuroHurst_wf and opts.write_graph:
        neuroHurst_wf.write_graph(graph2use="colored", format='svg', simple_form=True)

    retcode = retcode or int(neuroHurst_wf is None)
    if retcode != 0:
        sys.exit(retcode)

    # Check workflow for missing commands
    missing = check_deps(neuroHurst_wf)
    if missing:
        print("Cannot run uchicagoABCDProcessing. Missing dependencies:", file=sys.stderr)
        for iface, cmd in missing:
            print("\t{} (Interface: {})".format(cmd, iface))
        sys.exit(2)
    # Clean up master process before running workflow, which may create forks
    gc.collect()

    # Sentry tracking
    if not opts.notrack:
        from ..utils.sentry import start_ping
        start_ping(run_uuid, len(subject_list))

    return neuroHurst_wf, plugin_settings, opts, output_dir, work_dir, bids_dir, subject_list, run_uuid