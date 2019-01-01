from LoggingServices import makeLogger
from DataServices import DataServices
from NotificationServices import NotificationServices
from ComputeServices import ComputeServices
import WorkloadConstants

SNS_TOPIC = 'topic'
DYNAMODB_REGION = 'dynamoDBRegion'
DATA_SERVICES = 'dataServices'
SNS_TOPIC = 'topic'
NOTIFICATION_SERVICES = 'notificationService'
COMPUTE_SERVICES = 'computeService'

class WorkloadProxyDelegate( object ):


  def __init__( self, logLevelStr ):
    self.logger = makeLogger( __name__, logLevelStr );

  def initializeRequestState( self, paramsList ):
    if( DYNAMODB_REGION in paramsList):
      self.dynamoDBRegion = paramsList[DYNAMODB_REGION];
    else:
      raise NameError('WorkloadProxyDelegate:initializeRequestState() DYNAMODB_REGION not provided in paramsList')
    
    if( DATA_SERVICES in paramsList):
      self.dataServices = paramsList[DATA_SERVICES];
    else:
      raise NameError('WorkloadProxyDelegate:initializeRequestState() DATA_SERVICES not provided in paramsList')

    if (SNS_TOPIC in paramsList):
      self.snsTopic = paramsList[SNS_TOPIC];
    else:
      raise NameError( 'WorkloadProxyDelegate:initializeRequestState() SNS_TOPIC not provided in paramsList' )

    if (NOTIFICATION_SERVICES in paramsList):
      self.notificationServices = paramsList[NOTIFICATION_SERVICES];
    else:
      raise NameError( 'WorkloadProxyDelegate:initializeRequestState() NOTIFICATION_SERVICES not provided in paramsList' )

    if (COMPUTE_SERVICES in paramsList):
      self.computeServices = paramsList[COMPUTE_SERVICES];
    else:
      raise NameError(
        'WorkloadProxyDelegate:initializeRequestState() COMPUTE_SERVICES not provided in paramsList' )


  ###
  def directiveListAllWorkloads( self, requestDict, responseDict ):
    try:
      responseDict[WorkloadConstants.RESULT_BODY] = self.dataServices.lookupWorkloads();
    except Exception as e:
      self.logger.error( 'Exception on directiveListAllWorkloads() call of: {}'.format( e ) )
      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST
    return (responseDict)

  ###
  def directiveListWorkload( self, requestDict, responseDict ):
    workloadName = requestDict[WorkloadConstants.REQUEST_PARAM_WORKLOAD];
    try:
      workloadSpec = self.dataServices.lookupWorkloadSpecification( workloadName );
      responseDict[WorkloadConstants.RESULT_BODY] = workloadSpec;
    except Exception as e:
      self.logger.error(
        'Exception on directiveListWorkload() for workload name {}, exception is: {}'.format( workloadName, e ) )
      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST
    return (responseDict)

  ###
  def directiveActionWorkload( self, requestDict, responseDict ):
    responseDict = self.directiveListWorkload( requestDict, responseDict );

    workloadSpec = responseDict[WorkloadConstants.RESULT_BODY];

    if (DataServices.WORKLOAD_REGION not in workloadSpec):
      self.logging.error( 'Workload does not have a {} in DynamoDB'.format( DataServices.WORKLOAD_REGION ) )
      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST
    else:
      # Extract Region from Workload
      region = workloadSpec[DataServices.WORKLOAD_REGION]

      # Initialize common services with per request info
      workloadName = requestDict[WorkloadConstants.REQUEST_PARAM_WORKLOAD]
      self.notificationServices.initializeRequestState( self.snsTopic, workloadName, region );
      self.computeServices.initializeRequestState( self.dataServices, self.notificationServices, region );

      directiveAction = requestDict[WorkloadConstants.REQUEST_DIRECTIVE]

      if (directiveAction == WorkloadConstants.REQUEST_DIRECTIVE_ACTION_STOP):
        instancesStopped = [];
        try:
          instancesStopped = self.computeServices.actionStopWorkload( workloadName, requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] );

        except Exception as e:
          self.logger.error(
            'Exception on directiveActionWorkload() for workload name {}, exception is: {}'.format( workloadName, e ) )
          responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST
      responseDict[WorkloadConstants.RESULT_BODY] = { "instancesStopped": instancesStopped }

      return (responseDict)
