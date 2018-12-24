import json
import logging
import os

from LoggingServices import makeLogger
from DataServices import DataServices
from NotificationServices import NotificationServices
from ComputeServices import ComputeServices


ACTION_START='Start'
ACTION_STOP='Stop'
ACTION_SCALE='Scale'
NO_METHOD_FOUND='NoMethodFound'

RESULT_STATUS_CODE = 'statusCode'
RESULT_BASE_64 = 'isBase64Encoded'
RESULT_HEADERS = 'headers'
RESULT_BODY = 'body'
RESULT_CODE_BAD_REQUEST = 400
RESULT_CODE_OK_REQUEST = 200
RESULT_WORKLOAD_SPEC = 'workloadSpec'

REQUEST_PARAM_WORKLOAD = 'workloadName'
REQUEST_PARAM_DRYRUN = 'dryRun'
REQUEST_PARAM_ACTION = 'action'

REQUEST_EVENT_WORKLOAD_KEY = 'workload'
REQUEST_EVENT_PATHPARAMETER_KEY = 'pathParameters'
REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY = 'queryStringParameters'


# setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger(__name__, logLevelStr);
# logger = makeLogger(__name__, logLevelStr);

# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataServices =  DataServices(dynamoDBRegion, logLevelStr);

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

  # Extract workload identifier / SpecName from APIG Proxy event type
  if( (REQUEST_EVENT_PATHPARAMETER_KEY in event) and (REQUEST_EVENT_WORKLOAD_KEY in event[REQUEST_EVENT_PATHPARAMETER_KEY]) ):
    workloadName = event[REQUEST_EVENT_PATHPARAMETER_KEY][REQUEST_EVENT_WORKLOAD_KEY];
    mergedParamsDict[REQUEST_PARAM_WORKLOAD] = workloadName;
  else:
    logging.error('Cant obtain workloadName. Bad event request {}'.format(event))
    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
    return(mergedParamsDict)


  # Default to no Dry Run
  mergedParamsDict[REQUEST_PARAM_DRYRUN] = False;

  # Get Query String Params
  if( event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY] != None):
    if( REQUEST_PARAM_ACTION in event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]):
      requestAction =  event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY][REQUEST_PARAM_ACTION]
      mergedParamsDict[REQUEST_PARAM_ACTION] = requestAction;
    else:
      logger.error('No Action specified to perform for workload {}'.format(workloadName))
      resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
      return(mergedParamsDict)

    if( REQUEST_PARAM_DRYRUN in event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]):
      # if it exists, set to True, otherwise False
      #dryRunFlag =  event['queryStringParameters'][REQUEST_PARAM_DRYRUN]
      mergedParamsDict[REQUEST_PARAM_DRYRUN] = True;


  # Lookup workload details to extract region
  workloadSpec = dataServices.lookupWorkloadSpecification(workloadName);
  mergedParamsDict[RESULT_WORKLOAD_SPEC]=workloadSpec
  if( DataServices.WORKLOAD_REGION not in workloadSpec ):
    logging.error('Workload does not have a {} in DynamoDB'.format(DataServices.WORKLOAD_REGION))
    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
    return(mergedParamsDict);

  # Extract Region from Workload
  region = workloadSpec[DataServices.WORKLOAD_REGION]

  # Initialize common services with per request info
  notificationService.initializeRequestState(topic, workloadName, region);
  computeService.initializeRequestState(dataServices, notificationService, region);


  return(mergedParamsDict)



def lambda_handler(event, context):
  # Setup Lambda-->APIG Response
  # This is what is required for Proxy responses
  # {
  #   "isBase64Encoded": true | false,
  #   "statusCode": httpStatusCode,
  #   "headers": {"headerName": "headerValue", ...},
  #   "body": "..."
  # }
  resultResponseDict = {}
  resultResponseDict[RESULT_BASE_64]=False
  resultResponseDict[RESULT_STATUS_CODE]=RESULT_CODE_OK_REQUEST
  resultResponseDict[RESULT_HEADERS]={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
  resultResponseDict[RESULT_BODY]={}

  requestParamsDict = preprocessRequest(event, resultResponseDict)

  if(resultResponseDict[RESULT_STATUS_CODE] == RESULT_CODE_BAD_REQUEST):
    return(resultResponseDict);

  dryRunFlag = requestParamsDict[REQUEST_PARAM_DRYRUN]

  workloadName = requestParamsDict[REQUEST_PARAM_WORKLOAD]
  if(workloadName):
    if( REQUEST_PARAM_ACTION in requestParamsDict):
        action = requestParamsDict[REQUEST_PARAM_ACTION];
        if( action == ACTION_STOP):
          computeService.actionStopWorkload(workloadName, dryRunFlag);
    else: # No Action Specified, just return the spec
      resultResponseDict[RESULT_BODY]=requestParamsDict[RESULT_WORKLOAD_SPEC];


  # return workload details
  resultResponseDict[RESULT_BODY]=json.dumps(resultResponseDict[RESULT_BODY], indent=2);
  logger.info("Sending response of: " + json.dumps(resultResponseDict, indent=2));
  return (resultResponseDict);

if __name__ == "__main__":
  # /workloads/workload/BotoTestCase1?action=stop
  TestEvent = {
    "resource": "/workloads/{workload+}",
    "path": "/workloads/BotoTestCase1",
    "httpMethod": "GET",
    "headers": null,
    "multiValueHeaders": null,
    "queryStringParameters": {
        "action": "stop"
    },
    "multiValueQueryStringParameters": {
        "action": [
            "stop"
        ]
    },
    "pathParameters": {
        "workload": "BotoTestCase1"
    },
    "stageVariables": null,
    "requestContext": {
        "path": "/workloads/{workload+}",
        "accountId": "123456789012",
        "resourceId": "uzslb4",
        "stage": "test-invoke-stage",
        "domainPrefix": "testPrefix",
        "requestId": "bfaef07e-eaea-11e8-9b26-5774106da3f3",
        "identity": {
            "cognitoIdentityPoolId": null,
            "cognitoIdentityId": null,
            "apiKey": "test-invoke-api-key",
            "cognitoAuthenticationType": null,
            "userArn": "arn:aws:iam::123456789012:user/MyIAMName",
            "apiKeyId": "test-invoke-api-key-id",
            "userAgent": "aws-internal/3 aws-sdk-java/1.11.432 Linux/4.9.124-0.1.ac.198.71.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.181-b13 java/1.8.0_181",
            "accountId": "123456789012",
            "caller": "AIDAI3j42k4pfj2o2foij",
            "sourceIp": "test-invoke-source-ip",
            "accessKey": "ASIA3JFWLEFJOJVSOE3",
            "cognitoAuthenticationProvider": null,
            "user": "AIDAIJLJ4902JFJFS0J20"
        },
        "domainName": "testPrefix.testDomainName",
        "resourcePath": "/workloads/{workload+}",
        "httpMethod": "GET",
        "extendedRequestId": "QimohGPmPHcFarw=",
        "apiId": "dcamjxzqsh"
    },
    "body": null,
    "isBase64Encoded": false
}

  lambda_handler(TestEvent,{})

