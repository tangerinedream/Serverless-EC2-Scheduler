from LoggingServices import makeLogger
from DataServices import DataServices
from NotificationServices import NotificationServices
from ComputeServices import ComputeServices
import WorkloadConstants

SNS_TOPIC = 'topic'
DYNAMODB_REGION = 'dynamoDBRegion'
DATA_SERVICES = 'dataServices'
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
  def listAllWorkloads( self, requestDict, responseDict ):
    try:
      responseDict[WorkloadConstants.RESULT_BODY] = self.dataServices.lookupWorkloads();
    except Exception as e:
      self.logger.error( 'Exception on listAllWorkloads() call of: {}'.format( e ) )
      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

    responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST
    return (responseDict)

  ###
  def listWorkload( self, requestDict, responseDict ):
    workloadName = requestDict[WorkloadConstants.REQUEST_PARAM_WORKLOAD];
    try:
      workloadSpec = self.dataServices.lookupWorkloadSpecification( workloadName );
      responseDict[WorkloadConstants.RESULT_BODY] = workloadSpec;
    except Exception as e:
      self.logger.error(
        'Exception on listWorkload() for workload name {}, exception is: {}'.format( workloadName, e ) )
      responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

    responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST
    return (responseDict)

  ###
  def actionWorkload( self, requestDict, responseDict ):

    responseDict = self.listWorkload( requestDict, responseDict );

    if(responseDict[WorkloadConstants.RESULT_STATUS_CODE] == WorkloadConstants.RESULT_CODE_OK_REQUEST):

      # There should only be one element in the list returned.
      workloadSpec = responseDict[WorkloadConstants.RESULT_BODY][WorkloadConstants.WORKLOAD_RESULTS_KEY][0];

      if (DataServices.WORKLOAD_REGION not in workloadSpec):
        self.logging.error( 'Workload does not have a {} in DynamoDB'.format( DataServices.WORKLOAD_REGION ) )
        responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST
      else:
        # Extract Region from Workload
        region = workloadSpec[DataServices.WORKLOAD_REGION]

        # Initialize common services with per request info
        workloadName = requestDict[WorkloadConstants.REQUEST_PARAM_WORKLOAD]
        self.dataServices.initializeRequestState();
        self.notificationServices.initializeRequestState( self.snsTopic, workloadName, region );
        self.computeServices.initializeRequestState( self.dataServices, self.notificationServices, region );

        requestAction = requestDict[WorkloadConstants.REQUEST_DIRECTIVE]

        instancesActioned = [];
        try:

          # TODO: If Start or Stop, checkworkload state table to ensure workload is in valid state to execute operation

          if (requestAction == WorkloadConstants.REQUEST_DIRECTIVE_ACTION_STOP):
            instancesActioned = self.computeServices.actionStopWorkload( workloadName, requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] );
            if( requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] == False):
              self.dataServices.updateWorkloadStateTable(
                WorkloadConstants.ACTION_STOP,
                workloadName
              )
            responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST

          elif (requestAction == WorkloadConstants.REQUEST_DIRECTIVE_ACTION_START):
            # Was a Profile provided ?
            profileName=None
            if (WorkloadConstants.REQUEST_PARAM_PROFILE_NAME in requestDict):
              profileName = requestDict[WorkloadConstants.REQUEST_PARAM_PROFILE_NAME]
              instancesActioned = self.computeServices.actionStartWorkload(
                workloadName,
                requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN],
                profileName
              );
            else:
              instancesActioned = self.computeServices.actionStartWorkload(
                workloadName,
                requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN]
              );

            if( requestDict[WorkloadConstants.REQUEST_PARAM_DRYRUN] == False):
              self.dataServices.updateWorkloadStateTable(
                WorkloadConstants.ACTION_START,
                workloadName,
                profileName
              )

            # TODO: If no dryrun flag - update the workload state table to reflect updates state
            responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_OK_REQUEST

          else:
            self.logger.warning('In actionWorkload(). Unknown directive provided {} - no action is being taken'.format(requestAction))
            responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST

        except Exception as e:
          self.logger.error('Exception on actionWorkload() for workload name {}, exception is: {}'.format(
            workloadName,
            e )
          )
          responseDict[WorkloadConstants.RESULT_STATUS_CODE] = WorkloadConstants.RESULT_CODE_BAD_REQUEST
        responseDict[WorkloadConstants.RESULT_BODY] = {"InstancesActioned": instancesActioned}

      return (responseDict)

