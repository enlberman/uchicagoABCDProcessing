from nipype.interfaces.base import (
    traits, TraitedSpec, BaseInterfaceInputSpec, SimpleInterface,
    File, InputMultiPath, OutputMultiPath)
from nipype import logging
import cx_Oracle

LOGGER = logging.getLogger('nipype.interface')


class miNDARQueryInputSpec(TraitedSpec):
    subject_id = traits.String( mandatory=False, desc='subject identifier')
    username = traits.String(mandatory=True, desc='username for miNDAR')
    password = traits.String(mandatory=True, desc='passowrd for MINDAR')
    host = traits.String(mandatory=True, desc='host for miNDAR')


class miNDARQueryOutputSpec(TraitedSpec):
    out_report = File(exists=True, desc='conformation report')
    out = traits.List(value=[])


class miNDARQueryDFA(SimpleInterface):
    """

    """
    input_spec = miNDARQueryInputSpec
    output_spec = miNDARQueryOutputSpec

    def _run_interface(self, runtime):
        out=None
        try:
            connection: cx_Oracle.Connection = cx_Oracle.connect('%s/%s@%s/ORCL' % (self.inputs.username, self.inputs.password, self.inputs.host))
            cursor: cx_Oracle.Cursor = connection.cursor()
            query = ''
            cursor.execute(query)
            rows = cursor.fetchall()
            out = rows
        except cx_Oracle.DatabaseError as e:
            print("There was a problem with Oracle: ",  e)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        self._results['out'] = out

        return runtime
