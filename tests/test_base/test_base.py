import pytest
import time
import asyncio
from pathlib import Path
# from python_jdbc.database_connection import DatabaseConnection
# import python_jdbc
from pycore.load_config import main as load_config
import pycore.env as env

jdbc = None
connectionId = None
token = None

class TestJdbc:

    def test_select(self):
        global jdbc
        py_idh_dir = Path(__file__).parent.parent.parent / 'py_idh'
        with open(py_idh_dir / '.env.json') as env_file:
            load_config(env_file = env_file)
        from py_idh.database import PythonJdbc

        # use db connection through python_jdbc package --> connection will be registered in java server
        #db = DatabaseConnection(env.testMssql, 1, 1)
        # db.execute('SELECT TOP 1* FROM dbo.BSEG WHERE 1=2')
        print(PythonJdbc.execute(f"SELECT TOP 1* FROM dbo.BSEG", connection_data =  env.testMssql, token = env.httpToken))
        # pdb = PyEngineDatabaseConnection(connection_data, db.nbActiveConnections, db.nbOverflow, db.sqlAlchemyEngine)
        assert 0
    # def test_execution(self):
    #     print(jdbc.execute(f"INSERT INTO dbo.EKKO_EKPO_CLONE SELECT TOP 2* FROM dbo.EKPO", connection_id, token))

  