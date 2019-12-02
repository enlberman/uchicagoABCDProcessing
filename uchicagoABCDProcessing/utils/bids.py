#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities to handle BIDS inputs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
import os
import sys
import json
from pathlib import Path

from bids import BIDSLayout
from nipype import logging
from nipype.interfaces.base import OutputMultiObject
from niworkflows.interfaces.bids import _BIDSDataGrabberOutputSpec as BIDSDataGrabberOutputSpec
from niworkflows.interfaces.bids import _BIDSDataGrabberInputSpec as BIDSDataGrabberInputSpec
from niworkflows.interfaces.bids import BIDSDataGrabber

LOGGER = logging.getLogger('nipype.interface')


def write_derivative_description(bids_dir, deriv_dir):
    from ..__about__ import __version__, __url__, DOWNLOAD_URL

    bids_dir = Path(bids_dir)
    deriv_dir = Path(deriv_dir)
    desc = {
        'Name': 'uchicagoABCDProcessing - uchicago preprocessing of abcd data',
        'BIDSVersion': '1.1.1',
        'PipelineDescription': {
            'Name': 'uchicagoABCDProcessing',
            'Version': __version__,
            'CodeURL': DOWNLOAD_URL,
        },
        'CodeURL': __url__,
        # 'HowToAcknowledge':
        #     'Please cite our paper (https://doi.org/10.1038/s41592-018-0235-4), '
        #     'and include the generated citation boilerplate within the Methods '
        #     'section of the text.',
    }

    # Keys that can only be set by environment
    if 'uchicagoABCDProcessing'.upper() + '_DOCKER_TAG' in os.environ:
        desc['DockerHubContainerTag'] = os.environ['uchicagoABCDProcessing'.upper() +'_DOCKER_TAG']
    if 'uchicagoABCDProcessing'.upper() + '_SINGULARITY_URL' in os.environ:
        singularity_url = os.environ['uchicagoABCDProcessing'.upper() + '_SINGULARITY_URL']
        desc['SingularityContainerURL'] = singularity_url

        singularity_md5 = _get_shub_version(singularity_url)
        if singularity_md5 and singularity_md5 is not NotImplemented:
            desc['SingularityContainerMD5'] = _get_shub_version(singularity_url)

    # Keys deriving from source dataset
    orig_desc = {}
    fname = bids_dir / 'dataset_description.json'
    if fname.exists():
        with fname.open() as fobj:
            orig_desc = json.load(fobj)

    if 'DatasetDOI' in orig_desc:
        desc['SourceDatasetsURLs'] = ['https://doi.org/{}'.format(
            orig_desc['DatasetDOI'])]
    if 'License' in orig_desc:
        desc['License'] = orig_desc['License']

    with (deriv_dir / 'dataset_description.json').open('w') as fobj:
        json.dump(desc, fobj, indent=4)


def validate_input_dir(exec_env, bids_dir, participant_label):
    # Ignore issues and warnings that should not influence FMRIPREP
    import tempfile
    import subprocess
    validator_config_dict = {
        "ignore": [
            "EVENTS_COLUMN_ONSET",
            "EVENTS_COLUMN_DURATION",
            "TSV_EQUAL_ROWS",
            "TSV_EMPTY_CELL",
            "TSV_IMPROPER_NA",
            "VOLUME_COUNT_MISMATCH",
            "BVAL_MULTIPLE_ROWS",
            "BVEC_NUMBER_ROWS",
            "DWI_MISSING_BVAL",
            "INCONSISTENT_SUBJECTS",
            "INCONSISTENT_PARAMETERS",
            "BVEC_ROW_LENGTH",
            "B_FILE",
            "PARTICIPANT_ID_COLUMN",
            "PARTICIPANT_ID_MISMATCH",
            "TASK_NAME_MUST_DEFINE",
            "PHENOTYPE_SUBJECTS_MISSING",
            "STIMULUS_FILE_MISSING",
            "DWI_MISSING_BVEC",
            "EVENTS_TSV_MISSING",
            "TSV_IMPROPER_NA",
            "ACQTIME_FMT",
            "Participants age 89 or higher",
            "DATASET_DESCRIPTION_JSON_MISSING",
            "FILENAME_COLUMN",
            "WRONG_NEW_LINE",
            "MISSING_TSV_COLUMN_CHANNELS",
            "MISSING_TSV_COLUMN_IEEG_CHANNELS",
            "MISSING_TSV_COLUMN_IEEG_ELECTRODES",
            "UNUSED_STIMULUS",
            "CHANNELS_COLUMN_SFREQ",
            "CHANNELS_COLUMN_LOWCUT",
            "CHANNELS_COLUMN_HIGHCUT",
            "CHANNELS_COLUMN_NOTCH",
            "CUSTOM_COLUMN_WITHOUT_DESCRIPTION",
            "ACQTIME_FMT",
            "SUSPICIOUSLY_LONG_EVENT_DESIGN",
            "SUSPICIOUSLY_SHORT_EVENT_DESIGN",
            "MALFORMED_BVEC",
            "MALFORMED_BVAL",
            "MISSING_TSV_COLUMN_EEG_ELECTRODES",
            "MISSING_SESSION"
        ],
        "error": ["NO_T1W"],
        "ignoredFiles": ['/dataset_description.json', '/participants.tsv']
    }
    # Limit validation only to data from requested participants
    if participant_label:
        all_subs = set([s.name[4:] for s in bids_dir.glob('sub-*')])
        selected_subs = set([s[4:] if s.startswith('sub-') else s
                             for s in participant_label])
        bad_labels = selected_subs.difference(all_subs)
        if bad_labels:
            error_msg = 'Data for requested participant(s) label(s) not found. Could ' \
                        'not find data for participant(s): %s. Please verify the requested ' \
                        'participant labels.'
            if exec_env == 'docker':
                error_msg += ' This error can be caused by the input data not being ' \
                             'accessible inside the docker container. Please make sure all ' \
                             'volumes are mounted properly (see https://docs.docker.com/' \
                             'engine/reference/commandline/run/#mount-volume--v---read-only)'
            if exec_env == 'singularity':
                error_msg += ' This error can be caused by the input data not being ' \
                             'accessible inside the singularity container. Please make sure ' \
                             'all paths are mapped properly (see https://www.sylabs.io/' \
                             'guides/3.0/user-guide/bind_paths_and_mounts.html)'
            raise RuntimeError(error_msg % ','.join(bad_labels))

        ignored_subs = all_subs.difference(selected_subs)
        if ignored_subs:
            for sub in ignored_subs:
                validator_config_dict["ignoredFiles"].append("/sub-%s/**" % sub)
    with tempfile.NamedTemporaryFile('w+') as temp:
        temp.write(json.dumps(validator_config_dict))
        temp.flush()
        try:
            subprocess.check_call(['bids-validator', bids_dir, '-c', temp.name])
        except FileNotFoundError:
            print("bids-validator does not appear to be installed", file=sys.stderr)


def _get_shub_version(singularity_url):
    return NotImplemented


def collect_data(layout: BIDSLayout, subject_id, other_format: str = None):
    from niworkflows.utils.bids import collect_data
    standard_search, layout = collect_data(layout, subject_id)
    masks = [file.replace('preproc_bold','brain_mask') for file in standard_search['bold']]
    standard_search['mask'] = masks

    return standard_search, layout


class BIDSPlusDataGrabberOutputSpec(BIDSDataGrabberOutputSpec):
    csv = OutputMultiObject(desc='output csv')
    mask = OutputMultiObject(desc='output mask')


class BIDSPlusDataGrabber(BIDSDataGrabber):
    """
    Collect files from a BIDS directory structure plus support for other derivatives
    """

    input_spec = BIDSDataGrabberInputSpec
    output_spec = BIDSPlusDataGrabberOutputSpec
    _require_funcs = True
    _require_masks = False
    _require_anat = False

    def __init__(self, *args, **kwargs):
        # anat_only = kwargs.pop('anat_only')
        # require_t1w = kwargs.pop('require_t1w')
        require_masks = kwargs.pop('require_masks')
        super(BIDSDataGrabber, self).__init__(*args, **kwargs)
        # if anat_only is not None:
        #     self._require_funcs = not anat_only
        # if require_t1w is not None:
        #     self._require_anat = require_t1w
        if require_masks is not None:
            self._require_funcs = require_masks

    def _run_interface(self, runtime):
        bids_dict = self.inputs.subject_data

        self._results['out_dict'] = bids_dict
        self._results.update(bids_dict)

        if not bids_dict['t1w'] and self._require_anat:
            raise FileNotFoundError('No T1w images found for subject sub-{}'.format(
                self.inputs.subject_id))

        if self._require_funcs and not bids_dict['bold']:
            raise FileNotFoundError('No functional images found for subject sub-{}'.format(
                self.inputs.subject_id))

        if self._require_masks and not bids_dict['mask']:
            raise FileNotFoundError('No functional mask images found for subject sub-{}'.format(
                self.inputs.subject_id))

        for imtype in ['bold', 't2w', 'flair', 'fmap', 'sbref', 'roi', 'csv', 'mask']:
            if not bids_dict.keys().__contains__(imtype):
                LOGGER.warning('No "%s" images found for sub-%s',
                               imtype, self.inputs.subject_id)

        return runtime


