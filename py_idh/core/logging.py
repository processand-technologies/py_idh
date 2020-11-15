from .setup_logging import setup_logging
import py_idh.container as container

def error_handler(error, trace = None, label = None): 
    """
    throws an error message (GLOBAL LOGGER) and an error 

    :param error: logging message
    :param trace: logging message put in extra logging entry
    :param label: logging label
    """
    label = label + ' ' if label else ''
    # setup logging if necessary
    setup_logging()
    # trim messages
    if len(error) > 1800:
        error = error[:1200] + '\n...\n\n' + error[-400:].lstrip()
    if trace is not None and len(trace) > 2000:
        trace = trace[:600] + '\n...\n\n' + trace[-1000:].lstrip()
    # feed python logger 
    container.logger.error(label + error)
    if trace:
        container.logger.error(label + trace)
    if container.loggingLevel.lower() in ('debug', 'root'):
        container.logger_debug.error(label + error)
        if trace:
            container.logger_debug.error(label + trace)
    raise Exception (error)

def logging(level, label, message): 
    """
    throws a message

    :param level: logging level 
    :param label: logging label
    :param message: logging message
    """
    # setup logging if necessary
    setup_logging()
    if level != 'debug' and len(message) > 2000:
        message = message[:1500] + '\n...\n\n' + message[-600:].lstrip()
    elif level == 'debug' and len(message) > 4000:
        message = message[:2000] + '\n...\n\n' + message[-1500:].lstrip()
    # feed python logger 
    if level == 'info':
        container.logger.info(label + ' ' + message)
        if container.loggingLevel.lower() in ('debug', 'root'):
            container.logger_debug.info(label + ' ' + message)
    elif level == 'error':
        container.logger.error(label + ' ' + message)
        if container.loggingLevel.lower() in ('debug', 'root'):
            container.logger_debug.error(label + ' ' + message)
    elif level == 'warn' and container.loggingLevel.lower() in ('debug', 'root'):
        container.logger_debug.warning(label + ' ' + message)
    elif level == 'debug' and container.loggingLevel.lower() in ('debug', 'root'):
        container.logger_debug.debug(label + ' ' + message)
    