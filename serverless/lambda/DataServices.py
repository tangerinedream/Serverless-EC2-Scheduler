import boto3
import json
import re
import datetime

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from redo import retriable, retry  # See action function  https://github.com/mozilla-releng/redo

import WorkloadConstants
from LoggingServices import makeLogger


class DataServices(object):
  # Mapping of Python Class Variables to DynamoDB Attribute Names in Workload Table
  WORKLOAD_SPEC_TABLE_NAME = 'WorkloadSpecification'
  WORKLOAD_SPEC_PARTITION_KEY = 'SpecName'
  WORKLOAD_REGION= 'WorkloadRegion'

  WORKLOAD_STATE_TABLE='WorkloadState'
  WORKLOAD_STATE_TABLE_PARTITION_KEY = 'Workload'
  WORKLOAD_STATE_UNMANAGED='Unmanaged'

  TIER_SPEC_TABLE_NAME = 'TierSpecification'
  TIER_SPEC_PARTITION_KEY = 'SpecName'
  TIER_FILTER_TAG_KEY = 'TierFilterTagName'
  TIER_FILTER_TAG_VALUE = 'TierTagValue'
  TIER_NAME = 'TierTagValue'

  # Mapping of Python Class Variables to DynamoDB Attribute Names in Tier Table
  TIER_SEQ_NBR = 'TierSequence'
  TIER_SYCHRONIZATION = 'TierSynchronization'
  TIER_STOP_OVERRIDE_FILENAME = 'TierStopOverrideFilename'
  TIER_STOP_OS_TYPE = 'TierStopOverrideOperatingSystem'  # Valid values are Linux and Windows

  INTER_TIER_ORCHESTRATION_DELAY = 'InterTierOrchestrationDelay'  # The sleep time between commencing an action on this tier
  INTER_TIER_ORCHESTRATION_DELAY_DEFAULT = 5

  FLEET_SUBSET = 'FleetSubset'
  TIER_SCALING = 'TierScaling'
  TIER_SCALING_INSTANCE_TYPE = 'InstanceType'



  def __init__(self, dynamoDBRegion, logLevelStr):
    self.logger = makeLogger(__name__, logLevelStr);
    # self.logger.addHandler(logging.StreamHandler());
    self.dynamoDBRegion = dynamoDBRegion;

    self.dynDBC = self.makeDynamoDBConnection();

    self.dynDBR = self.makeDynamoDBResource();
    self.WorkloadStateTable = self.dynDBR.Table( DataServices.WORKLOAD_STATE_TABLE );
    self.dynDBDeserializer = boto3.dynamodb.types.TypeDeserializer();

    self.tierSpecTable = self.dynDBR.Table(DataServices.TIER_SPEC_TABLE_NAME)

    # Create a List of valid dynamoDB attributes to address user typos in dynamoDB table
    self.tierSpecificationValidAttributeList = [
      DataServices.TIER_FILTER_TAG_VALUE,
      DataServices.TIER_SPEC_TABLE_NAME,
      DataServices.TIER_SPEC_PARTITION_KEY,
      DataServices.TIER_NAME,
      WorkloadConstants.TIER_STOP,
      WorkloadConstants.TIER_START,
      WorkloadConstants.TIER_SCALING,
      WorkloadConstants.TIER_SEQ_NBR,
#     The following are not supported in the Serverless Version of the Scheduler
#       DataServices.TIER_SYCHRONIZATION,
#       DataServices.TIER_STOP_OVERRIDE_FILENAME,
#       DataServices.TIER_STOP_OS_TYPE,
      DataServices.INTER_TIER_ORCHESTRATION_DELAY
    ]

  def initializeRequestState( self ):
    self.tierSpecs = {}

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Tier Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupTierSpecs(self, tierIdentifier):
    '''
    Find all rows in table with partitionTargetValue
    Build a Dictionary (of Dictionaries).  Dictionary Keys are: TIER_START, TIER_STOP, TierScaleUp, TierScaleDown
    	Values are attributeValues of the DDB Item Keys
    '''
    tiersSpecificationDict = {}

    try:
      dynamodbItem = self.tierSpecTable.query(
        KeyConditionExpression=Key(DataServices.TIER_SPEC_PARTITION_KEY).eq(tierIdentifier),
        ConsistentRead=False,
      )
    except ClientError as e:
      self.logger.error('Exception encountered in lookupTierSpecs() -->' + str(e))
    else:
      # Was the item found in DynamoDB
      if ('Items' in dynamodbItem):
        # Get the dynamoDB Item from the result
        tiersAsItem = dynamodbItem['Items']


        # Create a Dictionary that stores the currTier and currTier associated with Tiers
        for currTier in tiersAsItem:
          self.logger.info('DynamoDB Query for Tier->' + currTier[DataServices.TIER_NAME])

          tierKeys = []
          self.recursiveFindKeys(currTier, tierKeys)
          setDiff = set(tierKeys).difference(self.tierSpecificationValidAttributeList)
          if (setDiff):
            for badAttrKey in setDiff:
              self.logger.warning('Invalid dynamoDB attribute specified->' + str(badAttrKey) + '<- will be ignored')

          # Add the dictionary element for this tier to return.
          tiersSpecificationDict[currTier[DataServices.TIER_NAME]] = {}

          # Create dict entry for each Tier
          # Pull out the Dictionaries for each of the sections below
          # Result is a key, and a dictionary
          if (WorkloadConstants.TIER_STOP in currTier):
            tiersSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {WorkloadConstants.TIER_STOP: currTier[WorkloadConstants.TIER_STOP]})

          if (WorkloadConstants.TIER_START in currTier):
            tiersSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {WorkloadConstants.TIER_START: currTier[WorkloadConstants.TIER_START]})

          if (WorkloadConstants.TIER_SCALING in currTier):
            tiersSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {WorkloadConstants.TIER_SCALING: currTier[WorkloadConstants.TIER_SCALING]})

            if (DataServices.FLEET_SUBSET in currTier):
              tiersSpecificationDict[currtier[DataServices.TIER_NAME]].update(
                {DataServices.FLEET_SUBSET: currTier[DataServices.FLEET_SUBSET]})

    # We hang on to the results for this request so we don't have to lookup several more times
    self.tierSpecs = tiersSpecificationDict
    return(tiersSpecificationDict);

  def getInterTierOrchestrationDelay( self, targetTierName, action ):
    delay = 0

    tierSpecsDict = self.tierSpecs

    for currTierName, tierAttributes in tierSpecsDict.items():
      self.logger.debug(
        'sequenceTiers() Action={}, currKey={}, currAttributes={})'.format( action, currTierName, tierAttributes ) )

      if( currTierName == targetTierName ):

        # This will be used to point to relevant dict within a specific tier's spec dict
        tierActionAttributes = {}

        if (action == WorkloadConstants.ACTION_STOP):
          # Locate the TIER_STOP Dictionary
          tierActionAttributes = tierAttributes[WorkloadConstants.TIER_STOP]

        elif (action == WorkloadConstants.ACTION_START):
          tierActionAttributes = tierAttributes[WorkloadConstants.TIER_START]

        if( DataServices.INTER_TIER_ORCHESTRATION_DELAY in tierActionAttributes):
          delayStr = tierActionAttributes[DataServices.INTER_TIER_ORCHESTRATION_DELAY]
          try:
            delay = float( delayStr )
          except ValueError:
            self.logger.warning('DataServices cannot convert InterTierOrchestrationDelay to a float().  Tier is {} Action is {}'.format(
              targetTierName,
              action
            ))

    return(delay)


  def getSequencedTierNames(self, workloadName, action):
    # get the tiers
    tierSpecsDict = self.tierSpecs
    if(not tierSpecsDict):
      tierSpecsDict = self.lookupTierSpecs( workloadName );


    # Prefill list for easy insertion
    length = len(tierSpecsDict);
    sequencedTierNameList = list(0 for i in range(length));


    # action indicates whether it is a TIER_STOP, or TIER_START, as they may have different sequences
    # Sequence is ascending
    for tierName, tierAttributes in tierSpecsDict.items():
      self.logger.debug('sequenceTiers() Action={}, currKey={}, currAttributes={})'.format(action, tierName, tierAttributes))

      # This will be used to point to relevant dict within a specific tier's spec dict
      tierActionAttributes = {}

      if (action == WorkloadConstants.ACTION_STOP):
        # Locate the TIER_STOP Dictionary
        tierActionAttributes = tierAttributes[WorkloadConstants.TIER_STOP]

      elif (action == WorkloadConstants.ACTION_START):
        tierActionAttributes = tierAttributes[WorkloadConstants.TIER_START]

      # Insert into the List
      idx = int(tierActionAttributes[WorkloadConstants.TIER_SEQ_NBR]);
      sequencedTierNameList[idx] = tierName;

    self.logger.debug('sequenceTiers() List for Action={} is {}'.format(action, sequencedTierNameList))

    return (sequencedTierNameList)


  def calculateFleetSubset( self, tierName, totalTierInstanceCount, profileName=None):

    #	We asume that there is no profile specified and that all instances will be started
    instancesToStart = totalTierInstanceCount

    if (profileName):
      self.logger.debug( 'fleetSubsetCalculation(): profileName {} supplied'.format(profileName) )

      # Unpack the ScalingDictionary
      tierAttributes = self.tierSpecs[tierName]
      if (DataServices.TIER_SCALING in tierAttributes):

        scalingDict = tierAttributes[DataServices.TIER_SCALING]
        self.logger.debug( 'FleetSubset for Tier {} {} '.format(tierName, str( scalingDict )) )

        if (profileName in scalingDict):
          if DataServices.FLEET_SUBSET in scalingDict[profileName]:
            fleetNumber = scalingDict[profileName][DataServices.FLEET_SUBSET]
            if re.search( "%", fleetNumber ):
              percentAsIntValue = fleetNumber.split( "%" )[0]
              if( int( percentAsIntValue == 0 ) ):
                instancesToStart=0
              elif (int( percentAsIntValue ) < 0) or (int( percentAsIntValue ) > 100):
                self.logger.info(
                  'FleetSubset specified out of range (less than 0i%% or more than 100%%) for profile [%s] and Tier [%s], starting all EC2 instances ' % (
                  str( self.scalingProfile ), tierName) )
              else:
                instancesToStart = int(
                  round( int( percentAsIntValue ) * totalTierInstanceCount / 100.0 ) )
                # In the event the calculated instances to start is < 1, which is cast to zero as an int,
                #   we set the instances to start explicitly to 1.  In the case where the calculation results in 20%
                #   for a tier, and there are only 2 instances in the tier, zero is calculated.  However, unless
                #   the user explicitly sets the percentage to zero (or is set to ordinal zero without a percentage), the
                #   assumption is the user does require some percentage of the tier to be used and therefore at least one
                #   instance will be started.
                if(instancesToStart == 0):
                  instancesToStart=1;
            else:
              instancesToStart = fleetNumber

        else:
          self.logger.warning( 'FleetSubset of [%s] not in tier [%s], will start all EC2 instances within a Tier ' % (
          str( self.scalingProfile ), tierName) )
      else:
        self.logger.warning(
          'FleetSubset Profile of [%s] specified but no FleetSubset dictionary found in DynamoDB for tier [%s]. Starting all EC2 instancew within a Tier' % (
          str( self.scalingProfile ), tierName) )

    return int(instancesToStart)

  def getTargetInstanceTypeForTierProfile( self, targetTierName, profileName ):
    targetInstanceType = None
    if(profileName is None):
      return(targetInstanceType);

    tierSpecsDict = self.tierSpecs

    for currTierName, tierAttributes in tierSpecsDict.items():
      self.logger.debug('getTargetInstanceTypeForTierProfile() currKey={}, currAttributes={})'.format(
        currTierName,
        tierAttributes
        )
      )

      if( currTierName == targetTierName ):

        # This will be used to point to relevant dict within a specific tier's spec dict
        if( WorkloadConstants.TIER_SCALING in tierAttributes):
          tierScalingAttributes = tierAttributes[WorkloadConstants.TIER_SCALING]
          if( profileName in tierScalingAttributes):
            targetInstanceType =  tierScalingAttributes[profileName][DataServices.TIER_SCALING_INSTANCE_TYPE]
            # Basic check to ensure targetInstanceType contains '.'
            if(re.search('.',targetInstanceType)):
              return(targetInstanceType)
            else:
              return(None)



  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Workload Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupWorkloadSpecification(self, workloadIdentifier):
    jsonWrapper = {}
    workloadsResultList = []
    workloadSpec = {}
    try:
      dynamodbItem = self.dynDBC.get_item(
        TableName=self.WORKLOAD_SPEC_TABLE_NAME,
        Key={
          self.WORKLOAD_SPEC_PARTITION_KEY: {"S": workloadIdentifier}
        },
        ConsistentRead=False,
      )

    except ClientError as e:
      self.logger.error('lookupWorkloadSpecification()' + e.response['Error']['Message'])
      return (workloadSpec);

    else:
      # Was the item found in DynamoDB
      if ('Item' in dynamodbItem):
        # Get the dynamoDB Item from the result
        workloadAsDynamoDBItem = dynamodbItem['Item']

        workloadSpec = self.dynamoDBItemToPythonDict(workloadAsDynamoDBItem)

    workloadsResultList.append(workloadSpec)
    jsonWrapper[WorkloadConstants.WORKLOAD_RESULTS_KEY]=workloadsResultList
    return( jsonWrapper );


  def lookupWorkloads(self):
    jsonWrapper = {}
    workloadsResultList = []

    try:
      dynamodbItems = self.dynDBC.scan(
        TableName=self.WORKLOAD_SPEC_TABLE_NAME,
        Select='ALL_ATTRIBUTES',
        ConsistentRead=False,
      )
    except ClientError as e:
      self.logger.error('lookupWorkloads()' + e.response['Error']['Message'])
      return(workloadsResultList)

    else:
      # Get the dynamoDB Item from the result
      workloadResultsListAsDynamoDBItems = dynamodbItems['Items'];

      # Strip out the DynamoDB value type dictionary
      for workloadAsDynamoDBItem in workloadResultsListAsDynamoDBItems:
        #workloadsResultList.append( self.workloadDynamoDBItemToPythonDict(workloadAsDynamoDBItem) );
        workloadsResultList.append( self.dynamoDBItemToPythonDict( workloadAsDynamoDBItem ) );

    jsonWrapper[WorkloadConstants.WORKLOAD_RESULTS_KEY]=workloadsResultList
    return (jsonWrapper);

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Workload State Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable( attempts=5, sleeptime=0, jitter=0 )
  def ensureWorkloadStateItemExists( self, workloadName ):
    currentTime = str( datetime.datetime.now().strftime( "%Y-%m-%d %H:%M:%S" ) )
    # ConditionExpression will make sure that item is put into DDB only if Workload item doesn't exist
    try:
      response = self.WorkloadStateTable.put_item(
        Item={
          'Workload': workloadName,
          'LastActionTime': str( currentTime ),
          'LastActionType': DataServices.WORKLOAD_STATE_UNMANAGED,
        },
        ConditionExpression='attribute_not_exists({})'.format(DataServices.WORKLOAD_SPEC_PARTITION_KEY)
      )

    except Exception as e:
      if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
        msg = 'ensureWorkloadCurrentStateExists() Exception encountered during DDB put_item {} -->'.format(e)
        logger.error( msg + str( e ) )
        raise e


  def updateWorkloadStateTable( self, action, workloadName, profileName=None ):

    self.ensureWorkloadStateItemExists(workloadName);

    currentTime = str( datetime.datetime.now().strftime( "%Y-%m-%d %H:%M:%S" ) )

    if (action == WorkloadConstants.ACTION_START) and (profileName is not None):
      UpdateExpressionAttr = 'SET Profile= :profile, LastActionTime= :currentTime, LastActionType= :actionType'
      ExpressionAttributeValuesAttr = {
        ':profile': profileName,
        ':currentTime': currentTime,
        ':actionType': action
      }

    else:
      UpdateExpressionAttr = 'SET LastActionTime= :currentTime, LastActionType= :actionType'
      ExpressionAttributeValuesAttr = {
        ':currentTime': currentTime,
        ':actionType': action,
      }

    try:
      retry( self.WorkloadStateTable.update_item, attempts=5, sleeptime=0, jitter=0, kwargs={
        "Key": {
          'Workload': workloadName,
        },
        "UpdateExpression": UpdateExpressionAttr,
        "ExpressionAttributeValues": ExpressionAttributeValuesAttr
      } )
    except Exception as e:
      msg = 'updateWorkloadStateTable() Exception encountered during DDB update {} -->'.format(e)
      logger.error( msg + str( e ) )


  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Utility Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # def workloadDynamoDBItemToPythonDict(self, dynamoDBWorkloadItem):
  #   workloadAsPythonDict = {};
  #
  #   for workloadAttrKey, workloadAttrVal in dynamoDBWorkloadItem.items():
  #     workloadVal = list( workloadAttrVal.values() )[0];  # new for python 3
  #     workloadAsPythonDict[workloadAttrKey] = workloadVal;
  #     self.logger.info( 'Workload Attribute [%s maps to %s]' % (workloadAttrKey, workloadVal) );
  #
  #   return(workloadAsPythonDict);

  def dynamoDBItemToPythonDict( self, dynamoDBItem ):
    result = {}
    for key, value in dynamoDBItem.items():
      result[key] = self.dynDBDeserializer.deserialize(value)
    return(result)

  def recursiveFindKeys(self, sourceDict, resList):
    for k, v in sourceDict.items():
      resList.append(k)
      if (isinstance(v, dict)):
        # Since scalingProfile key names are user dependent, we can't validate them
        if (k != WorkloadConstants.TIER_SCALING):
          self.recursiveFindKeys(v, resList)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Connection Factory
  # Please note:
  #   Unlike other Services, DataServices has no need to support multiple regions, as dynamoDB will always be accessed
  #   from a single region.  Workloads differ as one APIG + Lambda + DynamoDB deployment can act on workloads in any
  #   region.
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeDynamoDBConnection(self):
    try:
      self.logger.debug('obtaining boto3 dynamoDB client ');
      self.dynDBC = boto3.client('dynamodb', region_name=self.dynamoDBRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 dynamodb client in region %s -->' % self.dynamoDBRegion
      self.logger.error(msg + str(e));
    return (self.dynDBC);

  def getDynamoDBConnection(self):
    if (self.dynDBC):
      return (self.dynDBC)
    else:
      return (self.makeDynamoDBConnection());
    
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeDynamoDBResource(self):
    try:
      self.dynDBR = boto3.resource('dynamodb', region_name=self.dynamoDBRegion)
    except Exception as e:
      msg = 'Exception obtaining botot3 dynamodb resource in region %s -->' % self.dynamoDBRegion
      self.logger.error(msg + str(e));
    return (self.dynDBR);

  def getDynamoDBResource(self):
    if (self.dynDBR):
      return (self.dynDBR)
    else:
      return (self.makeDynamoDBResource());


if __name__ == "__main__":
  # setup logging service
  logLevel = logging.INFO

  # setup data service
  dataService = DataServices('us-west-2', logLevel);

  # Test lookupWorkloads()
  print("Testing lookupWorkloads()");
  workloadsRes = dataService.lookupWorkloads();
  print(json.dumps(workloadsRes, indent=2));

  # Test lookupWorkloads()
  print("Testing lookupWorkloadSpecification(SimpleWorkloadExample)");
  workloadRes = dataService.lookupWorkloadSpecification('SimpleWorkloadExample');
  print(json.dumps(workloadRes, indent=2));
  #print("Testing lookupWorkloads()");
  #workloadRes = dataService.lookupWorkloadSpecification('NoWorkloadName');
  #print(json.dumps(workloadRes, indent=2));

  print("Testing lookupTierSpecs()");
  tiersRes = dataService.lookupTierSpecs('BotoTestCase1');
  #tiersRes = dataService.lookupTierSpecs('SingleTierTest');
  print(json.dumps(tiersRes, indent=2));

