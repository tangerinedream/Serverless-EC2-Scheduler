import logging
import boto3
import json
from botocore.exceptions import ClientError

from boto3.dynamodb.conditions import Key, Attr

from redo import retriable, retry  # See action function  https://github.com/mozilla-releng/redo


class DataServices(object):
  # Mapping of Python Class Variables to DynamoDB Attribute Names in Workload Table
  WORKLOAD_SPEC_TABLE_NAME = 'WorkloadSpecification'
  WORKLOAD_SPEC_PARTITION_KEY = 'SpecName'

  TIER_SPEC_TABLE_NAME = 'TierSpecification'
  TIER_SPEC_PARTITION_KEY = 'SpecName'
  TIER_FILTER_TAG_KEY = 'TierFilterTagName'
  TIER_FILTER_TAG_VALUE = 'TierTagValue'
  TIER_NAME = 'TierTagValue'

  TIER_STOP = 'TierStop'
  TIER_START = 'TierStart'
  TIER_SCALING = 'TierScaling'

  # Mapping of Python Class Variables to DynamoDB Attribute Names in Tier Table
  TIER_SEQ_NBR = 'TierSequence'
  TIER_SYCHRONIZATION = 'TierSynchronization'
  TIER_STOP_OVERRIDE_FILENAME = 'TierStopOverrideFilename'
  TIER_STOP_OS_TYPE = 'TierStopOverrideOperatingSystem'  # Valid values are Linux and Windows

  INTER_TIER_ORCHESTRATION_DELAY = 'InterTierOrchestrationDelay'  # The sleep time between commencing an action on this tier
  INTER_TIER_ORCHESTRATION_DELAY_DEFAULT = 5

  FLEET_SUBSET = 'FleetSubset'


  def __init__(self, dynamoDBRegion, loglevel):
    self.logger = logging.getLogger(__name__);
    self.logger.setLevel(loglevel);
    self.logger.addHandler(logging.StreamHandler());
    self.dynamoDBRegion = dynamoDBRegion;

    self.dynDBC = self.makeDynamoDBConnection();

    self.dynDBR = self.makeDynamoDBResource();

    self.tierSpecTable = self.dynDBR.Table(DataServices.TIER_SPEC_TABLE_NAME)
    print('hello')

    # Create a List of valid dynamoDB attributes to address user typos in dynamoDB table
    self.tierSpecificationValidAttributeList = [
      DataServices.TIER_FILTER_TAG_VALUE,
      DataServices.TIER_SPEC_TABLE_NAME,
      DataServices.TIER_SPEC_PARTITION_KEY,
      DataServices.TIER_STOP,
      DataServices.TIER_START,
      DataServices.TIER_SCALING,
      DataServices.TIER_NAME,
      DataServices.TIER_SEQ_NBR,
      DataServices.TIER_SYCHRONIZATION,
      DataServices.TIER_STOP_OVERRIDE_FILENAME,
      DataServices.TIER_STOP_OS_TYPE,
      DataServices.INTER_TIER_ORCHESTRATION_DELAY
    ]

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupTierSpecs(self, tierIdentifier):
    '''
    Find all rows in table with partitionTargetValue
    Build a Dictionary (of Dictionaries).  Dictionary Keys are: TIER_START, TIER_STOP, TierScaleUp, TierScaleDown
    	Values are attributeValues of the DDB Item Keys
    '''
    tierSpecificationDict = {}

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
          tierSpecificationDict[currTier[DataServices.TIER_NAME]] = {}

          # Create dict entry for each Tier
          # Pull out the Dictionaries for each of the sections below
          # Result is a key, and a dictionary
          if (DataServices.TIER_STOP in currTier):
            tierSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {DataServices.TIER_STOP: currTier[DataServices.TIER_STOP]})

          if (DataServices.TIER_START in currTier):
            tierSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {DataServices.TIER_START: currTier[DataServices.TIER_START]})

          if (DataServices.TIER_SCALING in currTier):
            tierSpecificationDict[currTier[DataServices.TIER_NAME]].update(
              {DataServices.TIER_SCALING: currTier[DataServices.TIER_SCALING]})

            if (DataServices.FLEET_SUBSET in currTier):
              tierSpecificationDict[currtier[DataServices.TIER_NAME]].update(
                {DataServices.FLEET_SUBSET: currTier[DataServices.FLEET_SUBSET]})

    return(tierSpecificationDict);

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupWorkloadSpecification(self, workloadIdentifier):
    workloadSpecificationDict = {}

    try:
      # self.dynDBC = self.getDynamoDBConnection();

      dynamodbItem = self.dynDBC.get_item(
        TableName=self.WORKLOAD_SPEC_TABLE_NAME,
        Key={
          self.WORKLOAD_SPEC_PARTITION_KEY: {"S": workloadIdentifier}
        },
        ConsistentRead=False,
      )

    except ClientError as e:
      self.logger.error('lookupWorkloadSpecification()' + e.response['Error']['Message'])

    else:
      # Was the item found in DynamoDB
      if ('Item' in dynamodbItem):
        # Get the dynamoDB Item from the result
        workloadItem = dynamodbItem['Item']

        # Strip out the DynamoDB value type dictionary
        for attrKey, attrValueDynamo in workloadItem.items():
          attrValue = list(attrValueDynamo.values())[0];  # new for python 3.  Assumes data type is String
          self.logger.info('Workload Attribute [%s maps to %s]' % (attrKey, attrValue));
          workloadSpecificationDict[attrKey] = attrValue;

    return (workloadSpecificationDict);

  def lookupWorkloads(self):
    workloadsResultList = []

    try:
      # self.dynDBC = self.getDynamoDBConnection();

      dynamodbItems = self.dynDBC.scan(
        TableName=self.WORKLOAD_SPEC_TABLE_NAME,
        Select='ALL_ATTRIBUTES',
        ConsistentRead=False,
      )

    except ClientError as e:
      self.logger.error('lookupWorkloads()' + e.response['Error']['Message'])

    else:
      # Get the dynamoDB Item from the result
      workloadResultsList = dynamodbItems['Items'];

      # Strip out the DynamoDB value type dictionary
      for workload in workloadResultsList:

        currWorkloadDict = {};

        for workloadAttrKey, workloadAttrVal in workload.items():
          workloadVal = list(workloadAttrVal.values())[0];  # new for python 3
          currWorkloadDict[workloadAttrKey] = workloadVal;
          self.logger.info('Workload Attribute [%s maps to %s]' % (workloadAttrKey, workloadVal));

        workloadsResultList.append(currWorkloadDict)

    return (workloadsResultList);

  def recursiveFindKeys(self, sourceDict, resList):
    for k, v in sourceDict.items():
      resList.append(k)
      if (isinstance(v, dict)):
        # Since scalingProfile key names are user dependent, we can't validate them
        if (k != DataServices.TIER_SCALING):
          self.recursiveFindKeys(v, resList)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Connection Factory
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
  # print("Testing lookupWorkloadSpecification(SimpleWorkloadExample)");
  # workloadRes = dataService.lookupWorkloadSpecification('SimpleWorkloadExample');
  # print(json.dumps(workloadRes, indent=2));
  print("Testing lookupWorkloads()");
  workloadRes = dataService.lookupWorkloadSpecification('NoWorkloadName');
  print(json.dumps(workloadRes, indent=2));

  print("Testing lookupTierSpecs()");
  #tiersRes = dataService.lookupTierSpecs('BotoTestCase1');
  tiersRes = dataService.lookupTierSpecs('SingleTierTest');
  print(json.dumps(tiersRes, indent=2));

