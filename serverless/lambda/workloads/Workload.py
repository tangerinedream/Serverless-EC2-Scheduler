import json
from DataAbstraction import DynamoDBDataAbstractionService
import logging

#setup logging service
logger = logging.getLogger()
logLevel = logging.INFO

#setup data service
dataService = DynamoDBDataAbstractionService(logLevel);


def lambda_handler(event, context):

  # Informational logging
  logger.setLevel(logLevel);

  logger.info("Received event: " + json.dumps(event, indent=2));
  logger.info("Scheduler: workload = " + event['workload']);

  # Extract workload identifier
  workloadSpecName = event['workload'];

  # get data services
  if( dataService == nil ):
    dataService = DynamoDBDataAbstractionService();

  # Lookup workload details
  result = dataService.lookupWorkloadSpecification(workloadSpecName);

  # return workload details
  return (result);
