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
    sh = logging.StreamHandler();
    sh.setLevel(logLevel);
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s][%(message)s]');
    sh.setFormatter(formatter);
    logger.addHandler(sh);
  return (logger);
