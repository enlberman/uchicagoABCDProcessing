import os
from pathlib import Path
import logging
import sys
import gc
import warnings
from argparse import ArgumentParser
from argparse import ArgumentDefaultsHelpFormatter

from uchicagoABCDProcessing.cli.version import check_latest


def _warn_redirect(message, category, filename, lineno, logger, file=None, line=None):
    logger.warning('Captured warning (%s): %s', category, message)


def check_deps(workflow):
    from nipype.utils.filemanip import which
    return sorted(
        (node.interface.__class__.__name__, node.interface._cmd)
        for node in workflow._get_all_nodes()
        if (hasattr(node.interface, '_cmd') and
            which(node.interface._cmd.split()[0]) is None))


def get_parser() -> ArgumentParser:
    """Build parser object"""
    from fmriprep.cli.run import get_parser as fmriprep_get_parser
    parser = fmriprep_get_parser()
    # parser.add_argument('bids_dir', action='store', type=Path,
    #                     help='the root folder of a BIDS valid dataset (sub-XXXXX folders should '
    #                          'be found at the top level in this folder).')
    # parser.add_argument('output_dir', action='store', type=Path,
    #                     help='the output path for the outcomes of preprocessing and visual '
    #                          'reports')
    # parser.add_argument('analysis_level', choices=['participant'],
    #                     help='processing stage to be run, only "participant" in the case of '
    #                          'FMRIPREP (see BIDS-Apps specification).')
    #
    # # optional arguments
    # parser.add_argument('--version', action='version', version=verstr)
    #
    # g_bids = parser.add_argument_group('Options for filtering BIDS queries')
    # g_bids.add_argument('--skip_bids_validation', '--skip-bids-validation', action='store_true',
    #                     default=False,
    #                     help='assume the input dataset is BIDS compliant and skip the validation')

#     latest = check_latest()
#     if latest is not None and currentv < latest:
#         print("""\
# You are using uchicagoABCDProcessing-%s, and a newer version of uchicagoABCDProcessing is available: %s.
# Please check out our documentation about how and when to upgrade""" % (
#             __version__, latest), file=sys.stderr)
#
#     _blist = is_flagged()
#     if _blist[0]:
#         _reason = _blist[1] or 'unknown'
#         print("""\
# WARNING: Version %s of uchicagoABCDProcessing (current) has been FLAGGED
# (reason: %s).
# That means some severe flaw was found in it and we strongly
# discourage its usage.""" % (__version__, _reason), file=sys.stderr)

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