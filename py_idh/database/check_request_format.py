from requests import Session
import uuid
import json

token = ...
java_host = ...
java_port = ...
# task data that is to be handed over to jdbc with one adaption: 
# the attribute 'connectionData' needs to be added by the node backend
task_data = {
    'taskId': str(uuid.uuid4()),
    'command': 'execute',
    'params': {
        'query': 'SELECT TOP 1* FROM dbo.BSEG',
        'connectionId': ...,
        'useRedis': False, # new parameter for task to jdbc server - if False everything is emitted via ws
        'clientId': ..., # jdbc singleton's client id 
    }
}
headers = { 'authorization': token, 'Content-type': 'application/json' }
resp = Session().post(f"http://{java_host}:{java_port}/jdbc-server/addTask", data=json.dumps(task_data), headers = headers , timeout = 36000)