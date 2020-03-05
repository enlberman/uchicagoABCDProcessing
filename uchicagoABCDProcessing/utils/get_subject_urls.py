import numpy
import os
from pathlib import Path

import pandas
from nipype.interfaces.base import (
    traits, TraitedSpec, SimpleInterface,
    File)
from nipype import logging
import cx_Oracle

LOGGER = logging.getLogger('nipype.interface')


class OracleQueryInputSpec(TraitedSpec):
    username = traits.String(mandatory=True, desc='username for database')
    password = traits.String(mandatory=True, desc='passowrd for database')
    host = traits.String(mandatory=True, desc='host address for database')
    service = traits.String(mandatory=True, desc='database service')
    query = traits.String(mandator=True, desc='oracle sql query')
    write_to_file = traits.Bool(default_value=True, desc='write to output file')


class OracleQueryOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = traits.Any(exists=True, desc='result of database query')


class OracleQuery(SimpleInterface):
    """
    This is a simple interface for retrieving results of an sql query from an oracle database. The output is a pandas
    dataframe with query results. Note unless you build the column names into your query there are no column names
    in the resulting dataframe.
    """
    input_spec = OracleQueryInputSpec
    output_spec = OracleQueryOutputSpec

    def _run_interface(self, runtime):
        cursor = None
        connection = None
        try:
            connection: cx_Oracle.Connection = cx_Oracle.connect('%s/%s@%s/%s' % (self.inputs.username, self.inputs.password, self.inputs.host, self.inputs.service))
            """The format for the connection string is username/password@host/service"""

            cursor: cx_Oracle.Cursor = connection.cursor()  # connect
            cursor.execute(self.inputs.query)  # execute the sql
            rows = cursor.fetchall()  # retrieve the results
            out = pandas.DataFrame(rows)  # convert to pandas format
            if self.inputs.write_to_file:
                out_file = os.path.join(os.getcwd(), 'oracle_query_output.csv')  # make the output filename
                out.to_csv(out_file)  # write out the results
                self._results['out'] = out_file  # set the interface output
            else:
                self._results['out'] = out
        except cx_Oracle.DatabaseError as e:
            print("There was a problem with Oracle: ",  e)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        return runtime


nda_username='andrewstier'
nda_password='Almian817'
miNDAR_username='andrewstier_527849'
miNDAR_password='uchicagoabcd'
miNDAR_host='mindarvpc.cqahbwk3l1mb.us-east-1.rds.amazonaws.com'

participants=pandas.read_csv('/home/andrewstier/Downloads/ABCD_Subjects_201_NotPhillips.csv')['subjectkey'].values

os.system('export NDA_TOKEN_GEN_DIR=/home/andrewstier/nda_aws_token_generator')

aws_token_info = os.popen(
            "bash $NDA_TOKEN_GEN_DIR/curl/generate_token.sh '%s' '%s' 'https://nda.nih.gov/DataManager/dataManager'"
            % (nda_username, nda_password)
        ).readlines()
secret_key = aws_token_info[2].split(':')[1].strip()
access_key = aws_token_info[1].split(':')[1].strip()
session_token = aws_token_info[3].split(':')[1].strip()

paths = []
i=0
for participant in participants:
    print(i)
    i += 1
    try:
        get_files = OracleQuery()
        get_files.inputs.username = miNDAR_username
        get_files.inputs.password = miNDAR_password
        get_files.inputs.host = miNDAR_host
        get_files.inputs.service = 'ORCL'
        get_files.inputs.write_to_file=False

        get_files.inputs.query = "select column_name from USER_TAB_COLUMNS where table_name = 'FMRIRESULTS01'"
        get_files.run()
        columns = get_files._results['out'].values.flatten()
        scan_type = numpy.argwhere(columns == 'SCAN_TYPE').flatten()[0]
        file_link = numpy.argwhere(columns == 'DERIVED_FILES').flatten()[0]

        get_files.inputs.query = "select * from FMRIRESULTS01 where SUBJECTKEY = '%s'" % participant
        get_files.run()
        subject_files = get_files._results['out']

        anat_and_func_files =subject_files[(subject_files[scan_type] =='MR structural (T1)') | (subject_files[scan_type] =='fMRI')][file_link].values
        new_paths = numpy.unique([str(Path(x)) for x in anat_and_func_files])
        paths.append(new_paths)
        temp = numpy.unique(numpy.hstack(paths))
        paths=[]
        paths.append(temp)
        print()
    except:
        pass
    if i % 100 == 0:
        numpy.savetxt('/home/andrewstier/Downloads/subject_files.txt', paths[0],fmt='%s')
numpy.savetxt('/home/andrewstier/Downloads/subject_files.txt', paths[0],fmt='%s')