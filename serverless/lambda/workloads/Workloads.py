import json
import logging
from utils.DataAbstraction import DynamoDBDataAbstractionService

#setup logging service
logger = logging.getLogger();
logLevel = logging.INFO

#setup data service
dataService = DynamoDBDataAbstractionService(logLevel);


def lambda_handler(event, context):
  # Informational logging
  logger.setLevel(logLevel);

  logger.info("Received event: " + json.dumps(event, indent=2));


  # get data services
  global dataService
  if( dataService == None ):
    dataService = DynamoDBDataAbstractionService();

  # Lookup workload details
  result = dataService.lookupWorkloads();

  # return workloads as list of dictionaries
  return (
    {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'
      },
      'body': result,
    }
  );




