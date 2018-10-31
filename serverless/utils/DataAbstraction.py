import logging
import boto3
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
        logger.error(msg + str(e))
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
        logger.error('lookupWorkloadSpecification()' + e.response['Error']['Message'])
      else:
        # Get the dynamoDB Item from the result
        resultItem = dynamodbItem['Item']

        for attributeName in resultItem:
          # Validate the attributes entered into DynamoDB are valid.  If not, spit out individual warning messages
          if (attributeName in self.workloadSpecificationValidAttributeList):
            attributeValue = resultItem[attributeName].values()[0]
            logger.info('Workload Attribute [%s maps to %s]' % (attributeName, attributeValue))
            workloadSpecificationDict[attributeName] = attributeValue
          else:
            logger.warning('Invalid dynamoDB attribute specified->' + str(attributeName) + '<- will be ignored')

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
        logger.error('lookupWorkloads()' + e.response['Error']['Message'])

      else:
        # Get the dynamoDB Item from the result
        workloadResultsList = dynamodbItems['Items'];


        for workload in workloadResultsList:

          currWorkloadDict = {};

          for workloadAttrKey, workloadAttrVal in workload.items():
            workloadVal = list(workloadAttrVal.values())[0];       # new for python 3
            currWorkloadDict[workloadAttrKey] = workloadVal;  
            logger.info('Workload Attribute [%s maps to %s]' % (workloadAttrKey, workloadVal));

          workloadsResultList.append(currWorkloadDict)

      return(workloadsResultList);

if __name__ == "__main__":
  # setup logging service
  logger = logging.getLogger()
  logLevel = logging.INFO

  # setup data service
  dataService = DynamoDBDataAbstractionService(logLevel);
  dataService.lookupWorkloads();




#  Per Boto Docs
# #dynamodb = boto3.resource('dynamodb')
# #table = dynamodb.Table('name')
#
#
# #dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# #table = dynamodb.Table('elasticsearch-backups')
# #today = datetime.date.today()