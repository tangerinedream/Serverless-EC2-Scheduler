import json
from DataAbstraction import DynamoDBDataAbstractionService
import logging


logger = logging.getLogger()
logLevel = logging.INFO


def lambda_handler(event, context):
  # Set Logging
  logger.setLevel(logLevel);

  # get data services
  if( dataService == nil ):
    dataService = DynamoDBDataAbstractionService(logLevel);

  #dynamoDBConn = dao.getDynamoDBConnection()


  # return workload identifiers as list of dictionaries


  # print("Received event: " + json.dumps(event, indent=2))
  print("value1 = " + event['key1'])
  print("value2 = " + event['key2'])
  print("value3 = " + event['key3'])
  return event['key1']  # Echo back the first key value
  # raise Exception('Something went wrong')


# Per Boto Docs
#dynamodb = boto3.resource('dynamodb')
#table = dynamodb.Table('name')


#dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
#table = dynamodb.Table('elasticsearch-backups')
#today = datetime.date.today()
