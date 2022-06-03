# py_idh package

© 2021 Lyntics GmbH

Official Lyntics python package. This package allows you to access Lyntics functions via Python. If you use this package outside of Lyntics, you need to add host and port specifications accordingly.

--
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

3. install from the tar/whl file saved locally
```bash
pip install py_idh-1.0.1-py3-none-any.whl
pip install py_idh-1.0.1.tar.gz
```
The tar/whl files are created (updated) using
```bash
python setup.py sdist bdist_wheel
```

--
## Usage

1. Standard usage
```python
from py_idh.database import PythonJdbc

user_token = "..." 
connection_id = "..."

PythonJdbc.execute(
    "SELECT TOP 3* FROM dbo.BSEG", 
    connection_id = connection_id, 
    token = user_token
)
```

2. Advanced: Direct connection to the Lyntics JDBC server
```python
from py_idh.database import PythonJdbc

jdbc_token = "..."
mssql =  {
    "id": ...
}

PythonJdbc.execute(
    "SELECT TOP 3* FROM dbo.BSEG", 
    connection_data = mssql, 
    jdbc_token = jdbc_token
)
```