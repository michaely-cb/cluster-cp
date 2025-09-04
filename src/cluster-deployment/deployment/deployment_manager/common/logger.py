import logging
from logging.handlers import RotatingFileHandler
import sys

# Create the root logger
root = logging.getLogger()
root.setLevel(logging.DEBUG)

# Create the logger handlers
stderr_handler = logging.StreamHandler(sys.stderr)
file_handler = RotatingFileHandler("/dev/null")

# Add the handlers to the root logger
root.addHandler(stderr_handler)
root.addHandler(file_handler)

def force_log(module_logger, level, message, root_logger=None):
  """ 
  Custom logging handler to log messages to the file handler
  even if the log level is lower than the file handler's level.
  """
  if not root_logger:
    root_logger = root
  file_handler = root_logger.handlers[1]
  if level < file_handler.level:
    record = module_logger.makeRecord(module_logger.name, level, file_handler.name, 0, message, None, None)
    file_handler.emit(record)
  else:
    if level == logging.DEBUG:
        module_logger.debug(message)
    elif level == logging.INFO:
        module_logger.info(message)
    elif level == logging.WARNING:
        module_logger.warning(message)
    else:
        module_logger.error(message)
