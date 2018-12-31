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
RESULT_LIST_ALL_WORKLOADS_REQUEST = 'listAllWorkloads'

REQUEST_PARAM_WORKLOAD = 'workloadName'
REQUEST_PARAM_DRYRUN = 'dryRun'
REQUEST_PARAM_ACTION = 'action'

REQUEST_EVENT_WORKLOAD_KEY = 'workload'
REQUEST_EVENT_PATHPARAMETER_KEY = 'pathParameters'
REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY = 'queryStringParameters'

# What is the request trying to do ?
REQUEST_DIRECTIVE = 'directive'
REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS = 'listAllWorkloads'
REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC = 'listWorkload'
REQUEST_DIRECTIVE_ACTION_STOP = 'stopAction'
REQUEST_DIRECTIVE_ACTION_START = 'startAction'
REQUEST_DIRECTIVE_UNKNOWN = 'directiveUnknown'


# setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger(__name__, logLevelStr);

# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataServices =  DataServices(dynamoDBRegion, logLevelStr);

# Instantiation of Notification Services
topic = os.environ['SNS_TOPIC']
notificationService = NotificationServices(logLevelStr);

# Instantiation of Compute Services
computeService = ComputeServices(logLevelStr);


# def makeLogger():
#   logger = logging.getLogger(__name__)
#
#   loggingMap = {
#     'logging.CRITICAL' : 50,
#     'logging.ERROR' : 40,
#     'logging.WARNING' : 30,
#     'logging.INFO': 20,
#     'logging.DEBUG': 10,
#     'logging.NOTSET': 0,
#   }
#   logLevel = loggingMap['logging.INFO'];  # default to INFO
#
#   logLevelStr = os.environ['LOG_LEVEL']
#   if(logLevelStr in loggingMap):
#     logLevel = logginMap[logLevelStr];
#
#   logger.setLevel(logLevel);
#   return(logger);

def directiveListAllWorkloads(directiveRequest, resultResponseDict):
  try:
    resultResponseDict[RESULT_BODY] = dataServices.lookupWorkloads();
  except Exception as e:
    logger.error('Exception on directiveListAllWorkloads() call of: {}'.format(e))
    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST

  resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_OK_REQUEST
  return(resultResponseDict)


def directiveListWorkload(directiveRequest, resultResponseDict):
  workloadName = directiveRequest[REQUEST_PARAM_WORKLOAD];
  try:
    workloadSpec = dataServices.lookupWorkloadSpecification(workloadName);
    resultResponseDict[RESULT_BODY] =  workloadSpec;
  except Exception as e:
    logger.error('Exception on directiveListWorkload() for workload name {}, exception is: {}'.format(workloadName, e))
    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST

  resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_OK_REQUEST
  return(resultResponseDict)




def directiveActionWorkload(directiveRequest, resultResponseDict):
  resultResponseDict = directiveListWorkload(directiveRequest, resultResponseDict);

  workloadSpec = resultResponseDict[RESULT_BODY];

  if( DataServices.WORKLOAD_REGION not in workloadSpec ):
    logging.error('Workload does not have a {} in DynamoDB'.format(DataServices.WORKLOAD_REGION))
    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
  else:
    # Extract Region from Workload
    region = workloadSpec[DataServices.WORKLOAD_REGION]

    # Initialize common services with per request info
    workloadName =  directiveRequest[REQUEST_PARAM_WORKLOAD]
    notificationService.initializeRequestState(topic, workloadName, region);
    computeService.initializeRequestState(dataServices, notificationService, region);

    directiveAction = directiveRequest[REQUEST_DIRECTIVE]

    if( directiveAction == REQUEST_DIRECTIVE_ACTION_STOP):
      instancesStopped = [];
      try:
        instancesStopped = computeService.actionStopWorkload(workloadName, directiveRequest[REQUEST_PARAM_DRYRUN]);

      except Exception as e:
        logger.error(
          'Exception on directiveActionWorkload() for workload name {}, exception is: {}'.format(workloadName, e))
        resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST

    resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_OK_REQUEST
    resultResponseDict[RESULT_BODY] = {
      "instancesStopped" : instancesStopped
    }
    return (resultResponseDict)

def directiveUnknown(directiveRequest, resultResponseDict):
  logger.error('Unknown directive.  request info {}, result info {}'.format(directiveRequest, resultResponseDict))
  resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
  return (resultResponseDict)

def deriveDirective(event, resultResponseDict):
  # return a merged dictionary of http request params (path and query string parameters)
  mergedParamsDict = {}

  # Default to no Dry Run
  mergedParamsDict[REQUEST_PARAM_DRYRUN] = False;
  if( ((event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) is not None) and
      (REQUEST_PARAM_DRYRUN in event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) ):
    mergedParamsDict[REQUEST_PARAM_DRYRUN] = True;

  # If no workload specified in REQUEST_EVENT_PATHPARAMETER_KEY, return all workload specs as a list
  if( (REQUEST_EVENT_PATHPARAMETER_KEY in event) and (event[REQUEST_EVENT_PATHPARAMETER_KEY]) is None ):
    logging.info('Directive is {}. Returning list of all workload specs'.format(REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS));
    mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS;

  # Path Param of some sort was specified
  else:
    # Was it a Workload Identifier specified ?
    if( (REQUEST_EVENT_PATHPARAMETER_KEY in event) and
        (REQUEST_EVENT_WORKLOAD_KEY in event[REQUEST_EVENT_PATHPARAMETER_KEY]) ):

      # Collect the workload identifier
      workloadName = event[REQUEST_EVENT_PATHPARAMETER_KEY][REQUEST_EVENT_WORKLOAD_KEY];
      mergedParamsDict[REQUEST_PARAM_WORKLOAD] = workloadName;

      # Was Query String Param specified ?
      if (event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY] is not None):
        # Was Query String Param an Action  ?
        if (REQUEST_PARAM_ACTION in event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]):
          requestAction = event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY][REQUEST_PARAM_ACTION]
          mergedParamsDict[REQUEST_PARAM_ACTION] = requestAction;
          # Was it a Stop or Start or ?
          if(requestAction == ACTION_STOP ):
            mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_ACTION_STOP;
          elif(requestAction == ACTION_START):
            mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_ACTION_START
        else:
          # Don't know what the Query String is, bad request
          logger.warning('Invalid request: {} query string not present in request for workload {}'.format(REQUEST_PARAM_ACTION, workloadName))
          logger.warning('Request contained query string param of: {}'.format(event[REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]))
          mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_UNKNOWN

      # No Query String provided.  Return the Workload Spec for the provided Workload Identifier
      else:
        mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC


    # Path Param specified, but not a Workload Identifier
    else:
      logger.warning(
        'Invalid request: {} not present in {} request'.format(REQUEST_EVENT_WORKLOAD_KEY,
                                                                               REQUEST_EVENT_PATHPARAMETER_KEY))
      logging.warning('Cant obtain workloadName. Bad event request {}'.format(event))
      mergedParamsDict[REQUEST_DIRECTIVE] = REQUEST_DIRECTIVE_UNKNOWN

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
  # Informational logging
  logger.info("Received event: " + json.dumps(event, indent=2));

  resultResponseDict = {}
  resultResponseDict[RESULT_BASE_64]=False
  resultResponseDict[RESULT_STATUS_CODE]=RESULT_CODE_OK_REQUEST
  resultResponseDict[RESULT_HEADERS]={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
  resultResponseDict[RESULT_BODY]={}

  directiveSwitchStmt = {
    REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS: directiveListAllWorkloads,
    REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC: directiveListWorkload,
    REQUEST_DIRECTIVE_ACTION_STOP: directiveActionWorkload,
    REQUEST_DIRECTIVE_ACTION_START: directiveActionWorkload,
    REQUEST_DIRECTIVE_UNKNOWN: directiveUnknown
  }

  directiveRequest = deriveDirective(event, resultResponseDict)

  directive = directiveRequest[REQUEST_DIRECTIVE]

  # map request to directive function
  func=directiveSwitchStmt[directive];

  # invoke directive function.  Note: resultResponseDict will be updated with results
  resultResponseDict = func(directiveRequest, resultResponseDict);

  # return workload details
  # resultResponseDict[RESULT_BODY]=json.dumps(resultResponseDict[RESULT_BODY], indent=2);
  resultResponseDict[RESULT_BODY] = (str(resultResponseDict[RESULT_BODY]))   # body needs to be a string, not json
  logger.info("Sending response of: " + json.dumps(resultResponseDict, indent=2));
  return (resultResponseDict);

if __name__ == "__main__":

  ###
  # Example request types
  #
  # Return all workload specifications
  # /workloads
  #
  # Return specific workload specification
  # /workloads/workload/SampleWorkload-01
  #
  # Execute Stop Action on specific workload
  # /workloads/workload/SampleWorkload-01?action=Stop
  #
  ###


  ###
  # Test APIG Events
  TestListAllWorkloads = {
    "resource": "/workloads",
    "path": "/workloads",
    "httpMethod": "GET",
    "headers": {},
    "multiValueHeaders": {},
    "queryStringParameters": None,
    "multiValueQueryStringParameters": {
    },
    "pathParameters": None,
    "stageVariables": {},
    "requestContext": {
      "path": "/workloads/{workload+}",
      "accountId": "123456789012",
      "resourceId": "uzslb4",
      "stage": "test-invoke-stage",
      "domainPrefix": "testPrefix",
      "requestId": "bfaef07e-eaea-11e8-9b26-5774106da3f3",
      "identity": {
        "cognitoIdentityPoolId": {},
        "cognitoIdentityId": {},
        "apiKey": "test-invoke-api-key",
        "cognitoAuthenticationType": {},
        "userArn": "arn:aws:iam::123456789012:user/MyIAMName",
        "apiKeyId": "test-invoke-api-key-id",
        "userAgent": "aws-internal/3 aws-sdk-java/1.11.432 Linux/4.9.124-0.1.ac.198.71.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.181-b13 java/1.8.0_181",
        "accountId": "123456789012",
        "caller": "AIDAI3j42k4pfj2o2foij",
        "sourceIp": "test-invoke-source-ip",
        "accessKey": "ASIA3JFWLEFJOJVSOE3",
        "cognitoAuthenticationProvider": {},
        "user": "AIDAIJLJ4902JFJFS0J20"
      },
      "domainName": "testPrefix.testDomainName",
      "resourcePath": "/workloads/{workload+}",
      "httpMethod": "GET",
      "extendedRequestId": "QimohGPmPHcFarw=",
      "apiId": "dcamjxzqsh"
    },
    "body": {},
    "isBase64Encoded": False
  }

  TestListSampleWorkload01 = {
    "resource": "/workloads/{workload+}",
    "path": "/workloads/SampleWorkload-01",
    "httpMethod": "GET",
    "headers": {},
    "multiValueHeaders": {},
    "queryStringParameters": None,
    "multiValueQueryStringParameters": None,
    "pathParameters": {
      "workload": "SampleWorkload-01"
    },
    "stageVariables": {},
    "requestContext": {
      "path": "/workloads/{workload+}",
      "accountId": "123456789012",
      "resourceId": "uzslb4",
      "stage": "test-invoke-stage",
      "domainPrefix": "testPrefix",
      "requestId": "bfaef07e-eaea-11e8-9b26-5774106da3f3",
      "identity": {
        "cognitoIdentityPoolId": {},
        "cognitoIdentityId": {},
        "apiKey": "test-invoke-api-key",
        "cognitoAuthenticationType": {},
        "userArn": "arn:aws:iam::123456789012:user/MyIAMName",
        "apiKeyId": "test-invoke-api-key-id",
        "userAgent": "aws-internal/3 aws-sdk-java/1.11.432 Linux/4.9.124-0.1.ac.198.71.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.181-b13 java/1.8.0_181",
        "accountId": "123456789012",
        "caller": "AIDAI3j42k4pfj2o2foij",
        "sourceIp": "test-invoke-source-ip",
        "accessKey": "ASIA3JFWLEFJOJVSOE3",
        "cognitoAuthenticationProvider": {},
        "user": "AIDAIJLJ4902JFJFS0J20"
      },
      "domainName": "testPrefix.testDomainName",
      "resourcePath": "/workloads/{workload+}",
      "httpMethod": "GET",
      "extendedRequestId": "QimohGPmPHcFarw=",
      "apiId": "dcamjxzqsh"
    },
    "body": {},
    "isBase64Encoded": False
  }

  TestStopEvent = {
    "resource": "/workloads/{workload+}",
    "path": "/workloads/SampleWorkload-01",
    "httpMethod": "GET",
    "headers": {},
    "multiValueHeaders": {},
    "queryStringParameters": {
        "action": "Stop"
    },
    "multiValueQueryStringParameters": {
        "action": [
            "Stop"
        ]
    },
    "pathParameters": {
        "workload": "SampleWorkload-01"
    },
    "stageVariables": {},
    "requestContext": {
        "path": "/workloads/{workload+}",
        "accountId": "123456789012",
        "resourceId": "uzslb4",
        "stage": "test-invoke-stage",
        "domainPrefix": "testPrefix",
        "requestId": "bfaef07e-eaea-11e8-9b26-5774106da3f3",
        "identity": {
            "cognitoIdentityPoolId": {},
            "cognitoIdentityId": {},
            "apiKey": "test-invoke-api-key",
            "cognitoAuthenticationType": {},
            "userArn": "arn:aws:iam::123456789012:user/MyIAMName",
            "apiKeyId": "test-invoke-api-key-id",
            "userAgent": "aws-internal/3 aws-sdk-java/1.11.432 Linux/4.9.124-0.1.ac.198.71.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.181-b13 java/1.8.0_181",
            "accountId": "123456789012",
            "caller": "AIDAI3j42k4pfj2o2foij",
            "sourceIp": "test-invoke-source-ip",
            "accessKey": "ASIA3JFWLEFJOJVSOE3",
            "cognitoAuthenticationProvider": {},
            "user": "AIDAIJLJ4902JFJFS0J20"
        },
        "domainName": "testPrefix.testDomainName",
        "resourcePath": "/workloads/{workload+}",
        "httpMethod": "GET",
        "extendedRequestId": "QimohGPmPHcFarw=",
        "apiId": "dcamjxzqsh"
    },
    "body": {},
    "isBase64Encoded": False
}

  logger.info('Lambda function called __main__() !');

  ###
  # Executes various test cases
  # lambda_handler(TestListAllWorkloads,{})
  # lambda_handler(TestListSampleWorkload01,{})
  lambda_handler(TestStopEvent,{})

