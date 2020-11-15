import sys
import os
import logging as _logging
from logging import handlers

# global objects
import py_idh.container as container 

def setup_logging():
    """
    configure logging object

    :returns: logger (obj) 
    """
    if not hasattr(container, 'loggingLevel'):
        container.loggingLevel = 'info'
    formatter = _logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    log_file_path = 'logs'
    log_file_base_name = 'logging'
    # init info logger if not yet done
    if not isinstance(container.logger, _logging.Logger): 
        info_logger =  _logging.getLogger(' ')
        # handler = handlers.RotatingFileHandler(f'{log_file_path}//{log_file_base_name}.log', maxBytes=10*10**6, backupCount=2)
        # handler.setFormatter(formatter) 
        # info_logger.addHandler(handler)
        info_logger.setLevel(_logging.INFO)
        info_logger.info("Logger initialized")
        container.logger = info_logger
    # init debug logger if not yet done
    if container.loggingLevel.lower() in ('debug', 'root') and not isinstance(container.logger_debug, _logging.Logger):
        logger_debug =  _logging.getLogger('debug')
        # handler_debug = handlers.RotatingFileHandler(f'{log_file_path}//{log_file_base_name}_debug.log', maxBytes=container.loggingFileSize, backupCount=6)
        # handler_debug.setFormatter(formatter)
        # logger_debug.addHandler(handler_debug)
        logger_debug.setLevel(_logging.DEBUG)
        logger_debug.info("Debug Logger initialized")
        container.logger_debug = logger_debug
    # init root logger if not yet done
    if container.loggingLevel.lower() == 'root' and not isinstance(container.logger_root, _logging.Logger):
        logger_root = _logging.getLogger()
        # handler_root = handlers.RotatingFileHandler(f'{log_file_path}//{log_file_base_name}_root.log', maxBytes=container.loggingFileSize, backupCount=8)
        # handler_root.setFormatter(formatter)
        # logger_root.addHandler(handler_root)
        logger_root.setLevel(_logging.DEBUG)
        logger_root.info("Root Logger initialized")
        container.logger_root = logger_root

       