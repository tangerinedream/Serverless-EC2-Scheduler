import json
import logging
from utils.DataAbstraction import DynamoDBDataAbstractionService

# setup logging service
logger = logging.getLogger();
logLevel = logging.INFO

# setup data service
dataService = DynamoDBDataAbstractionService(logLevel);


def lambda_handler(event, context):
  # Informational logging
  logger.setLevel(logLevel);
  logger.info("Received event: " + json.dumps(event, indent=2));

  # Extract workload identifier
  workloadSpecName = event['params']['path']['workload'];
  logger.info("Scheduler: workload = " + workloadSpecName);

  # get data services
  global dataService
  if (dataService == None):
    dataService = DynamoDBDataAbstractionService();

  # Lookup workload details
  result = dataService.lookupWorkloadSpecification(workloadSpecName);

  # return workload details
  return (
    {
      'statusCode': 200,
      'headers': {
        'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'
      },
      'body': result,
    }
  );
