# initialize python server
from threading import Thread
import asyncio
import traceback
import time

from .jdbc import PythonJdbc as python_jdbc

def start_background_loop(_loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

try:
    loop = asyncio.new_event_loop()
    java_connection_thr = Thread(target = start_background_loop, args=(loop,), daemon=True)
    java_connection_thr.start()  
    PythonJdbc = python_jdbc.Instance()
    asyncio.run_coroutine_threadsafe(PythonJdbc.init(loop), loop)
    time.sleep(0.1) # config.yaml needs to be loaded
except:
    print("Cannot connect to java server\n" + traceback.format_exc())
    