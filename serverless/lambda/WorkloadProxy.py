import json
import yaml
import os
import datetime

import WorkloadConstants
from LoggingServices import makeLogger
from DataServices import DataServices
from NotificationServices import NotificationServices
from ComputeServices import ComputeServices
import WorkloadProxyDelegate

###
# These need to be outside of __main__ so they're accessible to Lambda
#

# setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger( __name__, logLevelStr );

# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataServices = DataServices( dynamoDBRegion, logLevelStr );

# Instantiation of Notification Services
topic = os.environ['SNS_TOPIC']
notificationServices = NotificationServices( logLevelStr );

# Instantiation of Compute Services
computeServices = ComputeServices( logLevelStr );


def dispatchUnknown(dispatchRequest, resultResponseDict):
  logger.error('Unknown dispatch.  request info {}, result info {}'.format(dispatchRequest, resultResponseDict))
  resultResponseDict[RESULT_STATUS_CODE] = RESULT_CODE_BAD_REQUEST
  return (resultResponseDict)

def deriveDispatch(event, resultResponseDict):
  # return a merged dictionary of http request params (path and query string parameters)
  mergedParamsDict = {}

  # Default to no Dry Run
  mergedParamsDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] = False;
  if( ((event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) is not None) and
      (WorkloadConstants.REQUEST_PARAM_DRYRUN in event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]) ):
    mergedParamsDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] = True;

  # If no workload specified in REQUEST_EVENT_PATHPARAMETER_KEY, return all workload specs as a list
  if( (WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY in event) and (event[WorkloadConstants.REQUEST_EVENT_PATHPARAMETER_KEY]) is None ):
    logger.info('Dispatch is {}. Returning list of all workload specs'.format(WorkloadConstants.REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS));
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
            mergedParamsDict[WorkloadConstants.REQUEST_DIRECTIVE] = WorkloadConstants.REQUEST_DIRECTIVE_ACTION_START;
            # If Start, was a Profile provided ?
            if(WorkloadConstants.REQUEST_PARAM_PROFILE_NAME in event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY]):
              profileName = event[WorkloadConstants.REQUEST_EVENT_QUERY_STR_PARAMETERS_KEY][WorkloadConstants.REQUEST_PARAM_PROFILE_NAME]
              mergedParamsDict[WorkloadConstants.REQUEST_PARAM_PROFILE_NAME] = profileName
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

  finishTime = 0
  startTime = datetime.datetime.now().replace( microsecond=0 )

  # Informational logging
  logger.info("Received event: " + json.dumps(event, indent=2));

  resultResponseDict = {}
  resultResponseDict[WorkloadConstants.RESULT_BASE_64]=False
  resultResponseDict[WorkloadConstants.RESULT_STATUS_CODE]=WorkloadConstants.RESULT_CODE_OK_REQUEST
  resultResponseDict[WorkloadConstants.RESULT_HEADERS]={
    'Content-Type'                : 'application/json',
    'Access-Control-Allow-Origin' : '*',
    'Access-Control-Allow-Headers': '*'
  }
  resultResponseDict[WorkloadConstants.RESULT_BODY]={}


  # Create delegate, who will orchestrate the processing of the dispatch
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

  # Switch map. Dispatch options, mapped to methods to invoke in the delegate.
  dispatchSwitchStmt = {
    WorkloadConstants.REQUEST_DIRECTIVE_LIST_ALL_WORKLOADS_SPECS: delegate.listAllWorkloads,
    WorkloadConstants.REQUEST_DIRECTIVE_LIST_WORKLOAD_SPEC: delegate.listWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_ACTION_STOP: delegate.actionWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_ACTION_START: delegate.actionWorkload,
    WorkloadConstants.REQUEST_DIRECTIVE_UNKNOWN: dispatchUnknown
  }

  # determine the requested directive
  dispatchRequest = deriveDispatch(event, resultResponseDict)

  # pull out the dispatch function indicator
  dispatch = dispatchRequest[WorkloadConstants.REQUEST_DIRECTIVE]


  # map request to dispatch function.  This is the equivilent of a Python Switch statement.
  func=dispatchSwitchStmt[dispatch];

  # invoke dispatch function.  Note: resultResponseDict will be updated with results
  resultResponseDict = func(dispatchRequest, resultResponseDict);

  logger.info('YAML version of output is: \n' + yaml.dump(resultResponseDict, indent=2, default_flow_style=False))
  # return APIG compatible results
  # Setup Lambda-->APIG Response
  # This is what is required for Proxy responses
  # {
  #   "isBase64Encoded": true | false,
  #   "statusCode": httpStatusCode,
  #   "headers": {"headerName": "headerValue", ...},
  #   "body": "..."
  # }
  # APIG needs body value as string
  resultResponseDict[WorkloadConstants.RESULT_BODY] = (json.dumps(resultResponseDict[WorkloadConstants.RESULT_BODY]))
  logger.info("Sending response to APIG (body as str required by APIG) of: " + json.dumps(resultResponseDict, indent=2));



  # capture completion time
  finishTime = datetime.datetime.now().replace( microsecond=0 )

  logger.info( '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++' )
  if( WorkloadConstants.REQUEST_PARAM_WORKLOAD in dispatchRequest ):
    workloadName = dispatchRequest[WorkloadConstants.REQUEST_PARAM_WORKLOAD]
    logger.info( '++ Completed processing [' + dispatch + '][' + workloadName + ']<- in ' + str(
      finishTime - startTime ) + ' seconds' )
  else:
    logger.info( '++ Completed processing [' + dispatch + ']<- in ' + str(finishTime - startTime ) + ' seconds' )
  logger.info( '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++' )

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

  TestStartEvent = {
    "resource": "/workloads/{workload+}",
    "path": "/workloads/SampleWorkload-01",
    "httpMethod": "GET",
    "headers": {},
    "multiValueHeaders": {},
    "queryStringParameters": {
      "action": "Start"
    },
    "multiValueQueryStringParameters": {
      "action": [
        "Start"
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

  TestStartEventWithProfile = {
    "resource": "/workloads/{workload+}",
    "path": "/workloads/SampleWorkload-01",
    "httpMethod": "GET",
    "headers": {},
    "multiValueHeaders": {},
    "queryStringParameters": {
      "action": "Start",
      "profileName": "DevMode"
    },
    "multiValueQueryStringParameters": {
      "action": [
        "Start"
      ],
      "profileName": [
        "DevMode", "HATestMode"
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
  # lambda_handler(TestStartEvent,{})
  # lambda_handler(TestStartEventWithProfile,{})
  lambda_handler(TestStopEvent,{})


