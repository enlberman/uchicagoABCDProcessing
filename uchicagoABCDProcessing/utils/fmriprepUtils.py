from argparse import ArgumentParser

from fmriprep.cli.run import get_parser as fmriprep_get_parser


def get_parser() -> ArgumentParser:
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
    return parser