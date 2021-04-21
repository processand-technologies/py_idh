import pytest
import time
import asyncio
from pathlib import Path
from pycore.load_config import main as load_config
import pycore.env as env
import py_idh.container as container

jdbc = None
connectionId = None
token = None

class TestJdbc:

    def test_select(self):
        global jdbc
        py_idh_dir = Path(__file__).parent.parent.parent / 'py_idh'
        print(py_idh_dir)
        with open(py_idh_dir / '.env.json') as env_file:
            load_config(env_file = env_file)
        
        container.javaHost = '127.0.0.1'
        from py_idh.database import PythonJdbc

        # use db connection through python_jdbc package --> connection will be registered in java server
        result = PythonJdbc.execute(f"SELECT TOP 1* FROM dbo.BSEG", host = '127.0.0.1', port = 3003, connection_data = env.testMssql, jdbc_token = env.httpToken)
        assert len(result.index) == 1

    def test_batch_insert(self):
        global jdbc
        # py_idh_dir = Path(__file__).parent.parent.parent / 'py_idh'
        # with open(py_idh_dir / '.env.json') as env_file:
        #     load_config(env_file = env_file)
        from py_idh.database import PythonJdbc

        # use db connection through python_jdbc package --> connection will be registered in java server
        result = PythonJdbc.batchStatement("INSERT INTO dbo.MINI_TABLE VALUES(?)", host = '127.0.0.1', port = 3003, connection_data =  env.testMssql, params = [['test'], ['None']], jdbc_token = env.httpToken)
        assert result.values.tolist() == [[1],[1]]