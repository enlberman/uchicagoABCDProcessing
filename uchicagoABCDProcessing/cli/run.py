# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
fractal scaling calculation workflow
=====
"""

import logging
import sys
from multiprocessing import set_start_method

logging.addLevelName(25, 'IMPORTANT')  # Add a new level between INFO and WARNING
logging.addLevelName(15, 'VERBOSE')  # Add a new level between INFO and DEBUG
logger = logging.getLogger('cli')


def main():
    """Entry point"""
    from .run_utils import get_workflow
    import sentry_sdk
    from ..utils.bids import write_derivative_description
    if __name__ == 'main':
        set_start_method('forkserver')
    errno = 1  # Default is error exit unless otherwise set
    workflow, plugin_settings, opts, output_dir, work_dir, bids_dir, subject_list, run_uuid = get_workflow(logger)

    try:
        workflow.run(**plugin_settings)
    except Exception as e:
        if not opts.notrack:
            from ..utils.sentry import process_crashfile
            crashfolders = [output_dir / 'uchicagoABCDProcessing' / 'sub-{}'.format(s) / 'log' / run_uuid
                            for s in subject_list]
            for crashfolder in crashfolders:
                for crashfile in crashfolder.glob('crash*.*'):
                    process_crashfile(crashfile)

            if "Workflow did not execute cleanly" not in str(e):
                sentry_sdk.capture_exception(e)
        logger.critical('uchicagoABCDProcessing failed: %s', e)
        raise
    else:
        errno = 0
        logger.log(25, 'uchicagoABCDProcessing finished without errors')
        if not opts.notrack:
            sentry_sdk.capture_message('uchicagoABCDProcessing finished without errors',
                                       level='info')
    finally:
        from niworkflows.reports import generate_reports
        from subprocess import check_call, CalledProcessError, TimeoutExpired
        from pkg_resources import resource_filename as pkgrf
        from shutil import copyfile

        citation_files = {
            ext: output_dir / 'uchicagoABCDProcessing' / 'logs' / ('CITATION.%s' % ext)
            for ext in ('bib', 'tex', 'md', 'html')
        }

        if citation_files['md'].exists():
            # Generate HTML file resolving citations
            cmd = ['pandoc', '-s', '--bibliography',
                   pkgrf('uchicagoABCDProcessing', 'data/boilerplate.bib'),
                   '--filter', 'pandoc-citeproc',
                   '--metadata', 'pagetitle="uchicagoABCDProcessing citation boilerplate"',
                   str(citation_files['md']),
                   '-o', str(citation_files['html'])]

            logger.info('Generating an HTML version of the citation boilerplate...')
            try:
                check_call(cmd, timeout=10)
            except (FileNotFoundError, CalledProcessError, TimeoutExpired):
                logger.warning('Could not generate CITATION.html file:\n%s',
                               ' '.join(cmd))

            # Generate LaTex file resolving citations
            cmd = ['pandoc', '-s', '--bibliography',
                   pkgrf('uchicagoABCDProcessing', 'data/boilerplate.bib'),
                   '--natbib', str(citation_files['md']),
                   '-o', str(citation_files['tex'])]
            logger.info('Generating a LaTeX version of the citation boilerplate...')
            try:
                check_call(cmd, timeout=10)
            except (FileNotFoundError, CalledProcessError, TimeoutExpired):
                logger.warning('Could not generate CITATION.tex file:\n%s',
                               ' '.join(cmd))
            else:
                copyfile(pkgrf('uchicagoABCDProcessing', 'data/boilerplate.bib'),
                         citation_files['bib'])
        else:
            logger.warning('uchicagoABCDProcessing could not find the markdown version of '
                           'the citation boilerplate (%s). HTML and LaTeX versions'
                           ' of it will not be available', citation_files['md'])

        # Generate reports phase
        failed_reports = generate_reports(
            subject_list, output_dir, work_dir, run_uuid, packagename='uchicagoABCDProcessing')
        write_derivative_description(bids_dir, output_dir / 'uchicagoABCDProcessing')

        if failed_reports and not opts.notrack:
            sentry_sdk.capture_message(
                'Report generation failed for %d subjects' % failed_reports,
                level='error')
        sys.exit(int((errno + failed_reports) > 0))


if __name__ == '__main__':
    raise RuntimeError("uchicagoABCDProcessing/cli/run.py should not be run directly;\n"
                       "Please `pip install` uchicagoABCDProcessing and use the `uchicagoABCDProcessing` command")
