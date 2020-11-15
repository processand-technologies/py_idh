import pytest
import websockets
import asyncio
import time
import json
import random
import pandas as pd

uri = "ws://127.0.0.1:3003/jdbc-server/websocket"

class TestWebsocketClient:
    @pytest.mark.asyncio
    async def test_start_websocket_client(self, samples):
        async with websockets.connect(uri) as websocket:
            client_id = '123'
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'execute',
            #         'clientId': client_id,
            #         'params': {'query': "SELECT * INTO \"dbo\".\"EKKO_EKPO_clone\" FROM (SELECT TOP 1000000 EKPO.* FROM \"dbo\".\"EKPO\" JOIN \"dbo\".\"EKKO\" ON EKPO.MANDT = EKKO.MANDT) AS SELECT_FROM"},
            #         'timeout': '1',
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s__JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })
            task = json.dumps({
                'task': {
                    'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
                    'command': 'getDbExplorerTables',
                    'clientId': client_id,
                    'params': {'schema': '%', 'catalog': '%ast%', 'limit': 1000, 'pattern': 'S%'},
                    'timeout': '10',
                    'connectionData': {
                        'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
                        'type': 'mssql',
                        'host': '52.57.77.16', 
                        'port': '1433', 
                        'user': 'a.mischi', 
                        'password': 'Process&2018', 
                        'database': 'SAPIndiaTables', 
                        'schema': 'dbo',
                        'url': None,
                    }
                },
                'authorization': "gy!s__JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            })

            task = json.dumps({
                'task': {
                    'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
                    'command': 'getDbMetaData',
                    'clientId': client_id,
                    'params': {'task': 'getIndexInfo', 'params': [None, None, 'EKPO', False, True]},
                    'timeout': '10',
                    'connectionData': {
                        'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
                        'type': 'mssql',
                        'host': '52.57.77.16', 
                        'port': '1433', 
                        'user': 'a.mischi', 
                        'password': 'Process&2018', 
                        'database': 'SAPIndiaTables', 
                        'schema': 'dbo',
                        'url': None,
                    }
                },
                'authorization': "gy!s__JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            })

            
            # t1 = time.time()
            # await websocket.send(task)
            # meta = await websocket.recv()
            # nbFrames = json.loads(meta)['nbFrames']
            # frame_output = []
            # for frame_nb in range(nbFrames):
            #     frame_output.append(await websocket.recv())
            # result = "".join(frame_output)
            # resultJson = json.loads(result)
            # resultDf = pd.DataFrame.from_records(resultJson["result"]["result"])
            # print("Time python takes to get 40.000 Rows of BSEG using Java Server (connection was not built before): ", time.time() - t1)
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'executePrep',
            #         'clientId': client_id,
            #         'params': {'query': 'SELECT TOP 1*, ? AS other FROM dbo.EKKO', 'params': [1], 'meta': ["getColumnLabel", "isNullable"]},
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s_'_JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'executeBatch',
            #         'clientId': client_id,
            #         'params': {'query': "INSERT INTO dbo.MINI_TABLE VALUES(?)", 'params': [['test'], [None]], 'meta': ["getColumnLabel", "isNullable"]},
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s__JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'executePrep',
            #         'clientId': client_id,
            #         'params': {'query': 'SELECT TOP 1000000* FROM dbo.EKKO', 'meta': ["getColumnLabel", "isNullable"], 'limit': 100},
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s_'_JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'getMemory',
            #         'clientId': client_id,
            #         'params': {'queries': ["""
            #             SELECT * INTO \"dbo\".\"EKKO_EKPO_clone\" 
            #             FROM (
            #                 SELECT TOP 1000000 EKPO.* FROM \"dbo\".\"EKPO\" JOIN \"dbo\".\"EKKO\" ON EKPO.MANDT = EKKO.MANDT
            #             ) AS SELECT_FROM"""], 'unit': "mb"},
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s_'_JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })
            # task = json.dumps({
            #     'task': {
            #         'taskId': '1231' + str(random.randint(10**4, 10**5-1)), 
            #         'command': 'executeBatch',
            #         'clientId': client_id,
            #         'params': {
            #             'query': "DELETE FROM dbo.EKPO WHERE MANDT = ? AND EBELN = ? AND EBELP = ?",
            #             'params': [[100 + i % 100, 100000 + i, 100 + i % 100] for i in range(100)]
            #             },
            #         'connectionData': {
            #             'id': '226a495a-b590-4b87-8883-00153762f89b',  # connection id
            #             'type': 'mssql',
            #             'host': '52.57.77.16', 
            #             'port': '1433', 
            #             'user': 'a.mischi', 
            #             'password': 'Process&2018', 
            #             'database': 'SAPIndiaTables', 
            #             'schema': 'dbo',
            #             'url': None,
            #         }
            #     },
            #     'authorization': "gy!s_'_JsUn/h3%h@#:FSwW$CG!^r$@-]{?KfL7kT].A37hg@k+TPKe!,2<u446`{?ff(!t7h66$3ptuL#_2V$(g~f,p8f]B8BfF$HhPjY?;;mn+PcRQvXXk-q(N",
            # })

            t1 = time.time()
            await websocket.send(json.dumps({'clientId': client_id}))
            reg_message = await websocket.recv()
            print(reg_message)
            for i in range(1):
                await websocket.send(task)
                first_message = await websocket.recv()
                first_output = json.loads(first_message)
                nb_frames = first_output['totalNbFrames']
                frame_output = [first_output['result']]
                for frame_nb in range(1, nb_frames):
                    frame_output.append(await websocket.recv())
                result = "".join(frame_output)
                print(result)
                print(len(result), time.time() - t1)
            # send dummy query to activate connection
            # samples['connection'].select("SELECT TOP 40* FROM dbo.BSEG")
            # t1 = time.time()
            # bseg = samples['connection'].select("SELECT TOP 40000* FROM dbo.BSEG")
            # print("Time python takes to get 40.000 Rows of BSEG (connection was already built before): ", time.time() - t1)

        assert 0
