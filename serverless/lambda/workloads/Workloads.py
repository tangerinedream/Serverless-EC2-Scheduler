import json
from DataAbstraction import DynamoDBDataAbstractionService
import logging


logger = logging.getLogger()
logLevel = logging.INFO
dataService = nil


def lambda_handler(event, context):
  # Informational logging
  logger.setLevel(logLevel);

  logger.info("Received event: " + json.dumps(event, indent=2));


  # get data services
  if( dataService == nil ):
    dataService = DynamoDBDataAbstractionService();

  # Lookup workload details
  result = dataService.lookupWorkloads();

  # return workload details
  return (result);
  # return workload identifiers as list of dictionaries



