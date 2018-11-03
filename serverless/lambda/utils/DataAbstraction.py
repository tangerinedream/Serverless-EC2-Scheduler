import logging
import boto3
import json
from botocore.exceptions import ClientError




class DynamoDBDataAbstractionService(object):
    # Mapping of Python Class Variables to DynamoDB Attribute Names in Workload Table
    WORKLOAD_SPEC_TABLE_NAME = 'WorkloadSpecification'
    WORKLOAD_SPEC_PARTITION_KEY = 'SpecName'

    def __init__(self, loglevel):
      self.dynamoDBRegion = "us-west-2";
      self.dynDBC  = None;
      self.logger  = logging.getLogger();
      self.logger.setLevel(loglevel);

    def makeDynamoDBConnection(self):
      try:
        self.dynDBC = boto3.client('dynamodb', region_name=self.dynamoDBRegion)
      except Exception as e:
        msg = 'Exception obtaining botot3 dynamodb client in region %s -->' % self.dynamoDBRegion
        self.logger.error(msg + str(e))
      return( self.dynDBC );


    def getDynamoDBConnection(self):
      # Check if connection open,
      #   return connection
      # else
      #   establish connection
      #   return connection
      if(self.dynDBC):
        return(self.dynDBC)
      else:
        return( self.makeDynamoDBConnection() );

    def lookupWorkloadSpecification(self, partitionTargetValue):
      workloadSpecificationDict = {}

      try:
        self.dynDBC = self.getDynamoDBConnection();

        dynamodbItem = self.dynDBC.get_item(
          TableName=self.WORKLOAD_SPEC_TABLE_NAME,
          Key={
            self.WORKLOAD_SPEC_PARTITION_KEY: {"S": partitionTargetValue}
          },
          ConsistentRead=False,
        )

      except ClientError as e:
        self.logger.error('lookupWorkloadSpecification()' + e.response['Error']['Message'])

      else:
        # Was the item found in DynamoDB
        if( 'Item' in dynamodbItem):
          # Get the dynamoDB Item from the result
          workloadItem = dynamodbItem['Item']

          # Strip out the DynamoDB value type dictionary
          for attrKey, attrValueDynamo in workloadItem.items():
            attrValue = list(attrValueDynamo.values())[0];       # new for python 3.  Assumes data type is String
            self.logger.info('Workload Attribute [%s maps to %s]' % (attrKey, attrValue));
            workloadSpecificationDict[attrKey] = attrValue;
            # workloadsResultList.append(currWorkloadDict)

      return (workloadSpecificationDict);


    def lookupWorkloads(self):
      workloadsResultList = []

      try:
        self.dynDBC = self.getDynamoDBConnection();

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
            workloadVal = list(workloadAttrVal.values())[0];       # new for python 3
            currWorkloadDict[workloadAttrKey] = workloadVal;  
            self.logger.info('Workload Attribute [%s maps to %s]' % (workloadAttrKey, workloadVal));

          workloadsResultList.append(currWorkloadDict)

      return(workloadsResultList);


if __name__ == "__main__":
  # setup logging service
  logger = logging.getLogger();
  logLevel = logging.INFO

  # setup data service
  dataService = DynamoDBDataAbstractionService(logLevel);

  # Test lookupWorkloads()
  print("Testing lookupWorkloads()");
  workloadsRes = dataService.lookupWorkloads();
  print(json.dumps(workloadsRes, indent=2));

  # Test lookupWorkloads()
  #print("Testing lookupWorkloadSpecification(SimpleWorkloadExample)");
  #workloadRes = dataService.lookupWorkloadSpecification('SimpleWorkloadExample');
  #print(json.dumps(workloadRes, indent=2));
  workloadRes = dataService.lookupWorkloadSpecification('NoWorkloadName');
  print(json.dumps(workloadRes, indent=2));




#  Per Boto Docs
# #dynamodb = boto3.resource('dynamodb')
# #table = dynamodb.Table('name')
#
#
# #dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# #table = dynamodb.Table('elasticsearch-backups')
# #today = datetime.date.today()