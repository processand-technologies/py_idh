# py_idh

You can either
1. pip install from git repo
```
pip install git+https://github.com/processand-technologies/py_idh
```
2. download package and pip install from directory
```
python install . (resp. path to module)
```    
3. or install from just the tar/whl file saved locally
```
pip install py_idh-0.1.2-py3-none-any.whl
pip install py_idh-0.1.2.tar.gz
```
these files are created (updated) using
```
python setup.py sdist bdist_wheel
```

Usage
1. standard usage
```
from py_idh.database import PythonJdbc
user_token = "..." 
PythonJdbc.execute("SELECT TOP 3* FROM dbo.BSEG", connection_id = ..., token = user_token)
```
2. direct connection to jdbc server
```
from py_idh.database import PythonJdbc
mssql =  {
        "id": ...
}
token = "..."
PythonJdbc.execute("SELECT TOP 3* FROM dbo.BSEG", connection_data = mssql, jdbc_token = token)
```