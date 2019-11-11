import os

import pandas
from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterfaceInputSpec, SimpleInterface,
    File, InputMultiPath, OutputMultiPath)
from nipype import logging
import cx_Oracle

LOGGER = logging.getLogger('nipype.interface')


class OracleQueryInputSpec(TraitedSpec):
    subject_id = traits.String( mandatory=False, desc='subject identifier')
    username = traits.String(mandatory=True, desc='username for database')
    password = traits.String(mandatory=True, desc='passowrd for database')
    host = traits.String(mandatory=True, desc='host address for database')
    service = traits.String(mandatory=False, default_value='ORCL', desc='database service')
    query = traits.String(mandator=True, desc='oracle sql query')


class OracleQueryOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = File(exists=True, desc='result of database query')


class OracleQuery(SimpleInterface):
    """
    This is a simple interface for retrieving results of an sql query from an oracle database. The output is
    """
    input_spec = OracleQueryInputSpec
    output_spec = OracleQueryOutputSpec

    def _run_interface(self, runtime):
        out = None
        cursor = None
        connection = None
        try:
            connection: cx_Oracle.Connection = cx_Oracle.connect('%s/%s@%s/%s' % (self.inputs.username, self.inputs.password, self.inputs.host, self.inputs.service))
            """The format for the connection string is username/password@host/service"""
            cursor: cx_Oracle.Cursor = connection.cursor()
            cursor.execute(self.inputs.query)
            rows = cursor.fetchall()
            out = pandas.DataFrame(rows)
            out.to_csv(os.path.join(os.getcwd(), 'oracle_query_output.csv'))
            self._results['out'] = out
        except cx_Oracle.DatabaseError as e:
            print("There was a problem with Oracle: ",  e)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        return runtime
