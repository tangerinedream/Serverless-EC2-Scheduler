import json
import logging
import os

from utils.LoggingServices import makeLogger
from utils.DataServices import DataServices
from utils.NotificationServices import NotificationServices
from utils.ComputeServices import ComputeServices

from workloads import WorkloadProxy

ACTION_START='actionStart'
ACTION_STOP='actionStop'
ACTION_SCALE='actionScale'
NO_METHOD_FOUND='NoMethodFound'

RESULT_STATUS_CODE = 'statusCode'
RESULT_HEADERS = 'headers'
RESULT_BODY = 'body'
RESULT_CODE_BAD_REQUEST = 400
RESULT_CODE_OK_REQUEST = 200

REQUEST_PARAM_WORKLOAD = 'workloadName'
REQUEST_PARAM_DRYRUN = 'dryRun'
REQUEST_PARAM_ACTION = 'action'


# setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger(__name__, logLevelStr);

# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataService =  DataServices(dynamoDBRegion, logLevelStr);

# Instantiation of Notification Services
topic = os.environ['SNS_TOPIC']
notificationService = NotificationServices(logLevelStr);

# Instantiation of Compute Services
computeService = ComputeServices(logLevelStr);

def makeLogger():
  logger = logging.getLogger(__name__)

  loggingMap = {
    'logging.CRITICAL' : 50,
    'logging.ERROR' : 40,
    'logging.WARNING' : 30,
    'logging.INFO': 20,
    'logging.DEBUG': 10,
    'logging.NOTSET': 0,
  }
  logLevel = loggingMap['logging.INFO'];  # default to INFO

  logLevelStr = os.environ['LOG_LEVEL']
  if(logLevelStr in loggingMap):
    logLevel = logginMap[logLevelStr];

  logger.setLevel(logLevel);
  return(logger);


def preprocessRequest(event, resultResponseDict):
  # return a merged dictionary of http request params (path and query string parameters)
  mergedParamsDict = {}

  # Informational logging
  logger.info("Received event: " + json.dumps(event, indent=2));

  # Extract workload identifier from APIG Proxy event type
  if( ('pathParameters' in event) and ('proxy' in event['pathParameters']) ):
    workloadName = event['pathParameters']['proxy'];
    mergedParamsDict[WorkloadProxy.REQUEST_PARAM_WORKLOAD] = workloadName;
  else:
    logging.error('Cant obtain workloadName. Bad event request {}'.format(event))
    resultResponseDict[WorkloadProxy.RESULT_STATUS_CODE] = WorkloadProxy.RESULT_CODE_BAD_REQUEST
    return(mergedParamsDict)


  # Get Query String Params
  if( WorkloadProxy.REQUEST_PARAM_ACTION in event['queryStringParameters']):
    requestAction =  event['queryStringParameters'][WorkloadProxy.REQUEST_PARAM_ACTION]
    mergedParamsDict[WorkloadProxy.REQUEST_PARAM_ACTION] = requestAction;
  else:
    logger.error('No Action specified to perform for workload {}'.format(workloadName))
    resultResponseDict[WorkloadProxy.RESULT_STATUS_CODE] = WorkloadProxy.RESULT_CODE_BAD_REQUEST
    return(mergedParamsDict)

  if( WorkloadProxy.REQUEST_PARAM_DRYRUN in event['queryStringParameters']):
    # if it exists, set to True, otherwise False
    #dryRunFlag =  event['queryStringParameters'][WorkloadProxy.REQUEST_PARAM_DRYRUN]
    mergedParamsDict[WorkloadProxy.REQUEST_PARAM_DRYRUN] = True;
  else:
    mergedParamsDict[WorkloadProxy.REQUEST_PARAM_DRYRUN] = False;

  # Lookup workload details to extract region
  workloadSpec = dataService.lookupWorkloadSpecification(workloadName);
  if( DataServices.WORKLOAD_REGION not in workloadSpec ):
    logging.error('Workload does not have a {} in DynamoDB'.format(DataServices.WORKLOAD_REGION))
    resultResponseDict[WorkloadProxy.RESULT_STATUS_CODE] = WorkloadProxy.RESULT_CODE_BAD_REQUEST
    return(mergedParamsDict);

  # Extract Region from Workload
  region = workloadSpec[DataServices.WORKLOAD_REGION]

  # Initialize common services with per request info
  notificationService.initializeRequestState(topic, workloadName, region);
  computeService.initializeRequestState(dataService, notificationService, region);


  return(mergedParamsDict)



def lambda_handler(event, context):
  # Setup Lambda-->APIG Response
  resultResponseDict = {}
  resultResponseDict[WorkloadProxy.RESULT_HEADERS]={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
  resultResponseDict[WorkloadProxy.RESULT_STATUS_CODE]=WorkloadProxy.RESULT_CODE_OK_REQUEST
  resultResponseDict[WorkloadProxy.RESULT_BODY]={}

# TODO: Determine if Python passes params by value or reference
  requestParamsDict = preprocessRequest(event, resultResponseDict)

  if(resultResponseDict[WorkloadProxy.RESULT_STATUS_CODE] == WorkloadProxy.RESULT_CODE_BAD_REQUEST):
    return(resultResponseDict);

  workloadName = requestParamsDict[WorkloadProxy.REQUEST_PARAM_WORKLOAD]
  dryRunFlag = requestParamsDict[WorkloadProxy.REQUEST_PARAM_DRYRUN]
  if(workloadName):
    computeService.actionStopWorkload(workloadName, dryRunFlag);


  # return workload details
  return (resultResponseDict);

if __name__ == "__main__":

  TestEvent = {
    "path": "/workloads/workload",
    "headers": {
    },
    "pathParameters": {
      "proxy": "BotoTestCase1"
    },
    "resource": "/{proxy+}",
    "httpMethod": "GET",
    "queryStringParameters": {
      "action": "stop"
    },
    "stageVariables": {
      "stageVarName": "dev"
    },
    "requestContext": {
      "accountId": "123456789012",
      "resourceId": "us4z18",
      "stage": "dev",
      "requestId": "41b45ea3-70b5-11e6-b7bd-69b5aaebc7d9",
      "identity": {
      },
      "resourcePath": "/{proxy+}",
      "httpMethod": "GET",
      "apiId": "wt6mne2s9k"
    }
  }

  lambda_handler(TestEvent,{})

