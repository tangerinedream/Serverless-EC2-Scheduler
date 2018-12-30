import json
import os

from LoggingServices import makeLogger
from DataServices import DataServices

#setup logging service
logLevelStr = os.environ['LOG_LEVEL']
logger = makeLogger(__name__, logLevelStr);


# setup services
dynamoDBRegion = os.environ['DYNAMODB_REGION']

# Instantiation of these services provide boto3 connection pools across lambda invocations
dataService =  DataServices(dynamoDBRegion, logLevel);

def lambda_handler(event, context):
  # Informational logging
  logger.info("Received event: " + json.dumps(event, indent=2));


  # get data services
  # global dataService
  # if( dataService == None ):
  #   dataService = DataServices(dynamoDBRegion, logLevel);

  # Lookup workload details
  workloads = str(dataService.lookupWorkloads());

  # return workloads as list of dictionaries
  return (
    {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'
      },
      'body': workloads,
    }
  );




