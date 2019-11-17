import re
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
    from fmriprep.cli.run import build_workflow as fmriprep_build_workflow

    build_log = nlogging.getLogger('nipype.workflow')

    INIT_MSG = """
    #     Running uchicagoABCDProcessing version {version}:
    #       * BIDS dataset path: {bids_dir}.
    #       * Participant list: {subject_list}.
    #       * Run identifier: {uuid}.
    #     """.format

    workflow = fmriprep_build_workflow(opts,retval)

    bids_dir = opts.bids_dir.resolve()
    output_dir = opts.output_dir.resolve()
    work_dir = opts.work_dir.resolve()

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

    return retval
