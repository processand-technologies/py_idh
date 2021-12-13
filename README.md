# py_idh package

© 2021 Lyntics GmbH

Lyntics python package. This package allows you to use your connections from within your python code. If you use this package outside of Lyntics, you need to add host and port accordingly.

## Installation

You can either
1. pip install from git repo
```bash
pip install git+https://github.com/processand-technologies/py_idh
```
2. download package and pip install from directory
```bash
python install . (resp. path to module)
```    
3. or install from just the tar/whl file saved locally
```bash
pip install py_idh-1.0.1-py3-none-any.whl
pip install py_idh-1.0.1.tar.gz
```
these files are created (updated) using
```bash
python setup.py sdist bdist_wheel
```

## Usage

1. standard usage
```python
from py_idh.database import PythonJdbc
user_token = "..." 
PythonJdbc.execute("SELECT TOP 3* FROM dbo.BSEG", connection_id = ..., token = user_token)
```
2. direct connection to jdbc server
```python
from py_idh.database import PythonJdbc
mssql =  {
        "id": ...
}
token = "..."
PythonJdbc.execute("SELECT TOP 3* FROM dbo.BSEG", connection_data = mssql, jdbc_token = token)
```