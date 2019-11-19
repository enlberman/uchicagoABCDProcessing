import os

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
