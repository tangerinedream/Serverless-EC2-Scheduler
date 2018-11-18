import logging


def makeLogger(name, logLevelStr):
  logger = logging.getLogger(name);

  loggingMap = {
    'logging.CRITICAL': 50,   
    'logging.ERROR': 40,
    'logging.WARNING': 30,
    'logging.INFO': 20,
    'logging.DEBUG': 10,
    'logging.NOTSET': 0,
  }


  if (logLevelStr in loggingMap):
    logLevel = loggingMap[logLevelStr];
  else:
    logLevel = loggingMap['logging.INFO'];  # default to INFO

  logger.setLevel(logLevel);
  if( not logger.hasHandlers() ):
    logger.addHandler(logging.StreamHandler());
  return (logger);
