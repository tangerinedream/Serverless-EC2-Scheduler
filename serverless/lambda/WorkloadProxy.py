import json
import os

import WorkloadConstants
from LoggingServices import makeLogger
from DataServices import DataServices
from NotificationServices import NotificationServices
from ComputeServices import ComputeServices
import WorkloadProxyDelegate


# setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger(__name__, logLevelStr);

# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataServices =  DataServices(dynamoDBRegion, logLevelStr);

# Instantiation of Notification Services
topic = os.environ['SNS_TOPIC']
notificationServices = NotificationServices(logLevelStr);

# Instantiation of Compute Services
computeServices = ComputeServices(logLevelStr);


def directiveUnknown(directiveRequest, resultResponseDict):
  logger.error('Unknown directive.  request info {}, result info {}'.format(directiveRequest, resultResponseDict))
  resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
  return (resultResponseDict)

def deriveDirective(event, resultResponseDict):
  # return a merged dictionary of http request params (path and query string parameters)
  mergedParamsDict = {}

  # Default to no Dry Run
  mergedParamsDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] = False;
  if( ((event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) is not None) and
      (WorkloadConstants.REQUEST_PARAM_DRYRUN in event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) ):
    mergedParamsDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] = True;

  # If no workload specified in REQUEST_EVENT_PATHPARAMETER_KEY, return all workload specs as a list
  if( (WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY in event) and (event[WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY]) is None ):
    logger.info('Directive is {}. Returning list of all workload specs'.format(WorkloadConstants.REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS));
    mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS;

  # Path Param of some sort was specified
  else:
    # Was it a Workload Identifier specified ?
    if( (WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY in event) and
        (WorkloadConstants.REQUEST_EVENT_WORKLOAD_KEY in event[WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY]) ):

      # Collect the workload identifier
      workloadName = event[WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY][WorkloadConstants.REQUEST_EVENT_WORKLOAD_KEY];
      mergedParamsDict[WorkloadConstants.REQUEST_PARAM_WORKLOAD] = workloadName;

      # Was Query String Param specified ?
      if (event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY] is not None):
        # Was Query String Param an Action  ?
        if (WorkloadConstants.REQUEST_PARAM_ACTION in event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]):
          requestAction = event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY][WorkloadConstants.REQUEST_PARAM_ACTION]
          mergedParamsDict[WorkloadConstants.REQUEST_PARAM_ACTION] = requestAction;
          # Was it a Stop or Start or ?
          if(requestAction == WorkloadConstants.ACTION_STOP ):
            mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_ACTION_STOP;
          elif(requestAction == WorkloadConstants.ACTION_START):
            mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_ACTION_START
        else:
          # Don't know what the Query String is, bad request
          logger.warning('Invalid request: {} query string not present in request for workload {}'.format(WorkloadConstants.REQUEST_PARAM_ACTION, workloadName))
          logger.warning('Request contained query string param of: {}'.format(event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]))
          mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_UNKNOWN

      # No Query String provided.  Return the Workload Spec for the provided Workload Identifier
      else:
        mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC


    # Path Param specified, but not a Workload Identifier
    else:
      logger.warning(
        'Invalid request: {} not present in {} request'.format(WorkloadConstants.REQUEST_EVENT_WORKLOAD_KEY,
                                                               WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY))
      logger.warning('Cant obtain workloadName. Bad event request {}'.format(event))
      mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_UNKNOWN

  return(mergedParamsDict)

def lambda_handler(event, context):

  # Informational logging
  logger.info("Received event: " + json.dumps(event, indent=2));

  resultResponseDict = {}
  resultResponseDict[WorkloadConstants.RESULT_BASE_64]=False
  resultResponseDict[WorkloadConstants.RESULT_STATUS_CODE]=WorkloadConstants.RESULT_CODE_OK_REQUEST
  resultResponseDict[WorkloadConstants.RESULT_HEADERS]={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
  resultResponseDict[WorkloadConstants.RESULT_BODY]={}


  # Create delegate, who will orchestrate the processing of the directive
  delegate = WorkloadProxyDelegate.WorkloadProxyDelegate( logLevelStr );

  # Initiatlize delegate for this specific request by creating the parameter dictionary to pass it
  # Create dictionary
  workloadProxyDelegateInitializeRequestStateParams = {
    WorkloadProxyDelegate.DYNAMODB_REGION : dynamoDBRegion,
    WorkloadProxyDelegate.DATA_SERVICES : dataServices,
    WorkloadProxyDelegate.SNS_TOPIC : topic,
    WorkloadProxyDelegate.NOTIFICATION_SERVICES : notificationServices,
    WorkloadProxyDelegate.COMPUTE_SERVICES : computeServices
  }
  # Initialize the state
  delegate.initializeRequestState( workloadProxyDelegateInitializeRequestStateParams );

  # Switch map. Directive options, mapped to methods to invoke in the delegate.
  directiveSwitchStmt = {
    WorkloadConstants.REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS: delegate.directiveListAllWorkloads,
    WorkloadConstants.REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC: delegate.directiveListWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_ACTION_STOP: delegate.directiveActionWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_ACTION_START: delegate.directiveActionWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_UNKNOWN: directiveUnknown
  }

  # determine the chosen directive
  directiveRequest = deriveDirective(event, resultResponseDict)

  # pull out the directive
  directive = directiveRequest[WorkloadConstants.REQUEST_DIRECTIVE]

  # map request to directive function.  This is the equivilent of a Python Switch statement.
  func=directiveSwitchStmt[directive];

  # invoke directive function.  Note: resultResponseDict will be updated with results
  resultResponseDict = func(directiveRequest, resultResponseDict);

  # return APIG compatible results
  # Setup Lambda-->APIG Response
  # This is what is required for Proxy responses
  # {
  #   "isBase64Encoded": true | false,
  #   "statusCode": httpStatusCode,
  #   "headers": {"headerName": "headerValue", ...},
  #   "body": "..."
  # }
  resultResponseDict[WorkloadConstants.RESULT_BODY] = (str(resultResponseDict[WorkloadConstants.RESULT_BODY]))   
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
  lambda_handler(TestListAllWorkloads,{})
  # lambda_handler(TestListSampleWorkload01,{})
  # lambda_handler(TestStopEvent,{})

