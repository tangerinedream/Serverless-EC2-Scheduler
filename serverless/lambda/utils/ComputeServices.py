import logging
import boto3
import json
from botocore.exceptions import ClientError

from redo import retriable, retry  # See action function  https://github.com/mozilla-releng/redo

# CloudWatch Logs
import watchtower
from NotificationServices import NotificationServices
#from utils import NotificationServices


class ComputeServices(object):
  WORKLOAD_SPEC_REGION_KEY = 'WorkloadRegion'

  WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY = 'WorkloadFilterTagName'
  WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE = 'WorkloadFilterTagValue'

  WORKLOAD_VPC_ID_KEY = 'VPC_ID'

  TIER_FILTER_TAG_KEY = 'TierFilterTagName'
  TIER_FILTER_TAG_VALUE = 'TierTagValue'

  # These are the official codes per Boto3
  BOTO3_INSTANCE_STATE_MAP = {
    0: "pending",
    16: "running",
    32: "shutting-down",
    48: "terminated",
    64: "stopping",
    80: "stopped"
  }

  def __init__(self, snsService, workloadRegion, loglevel):
    self.logger = logging.getLogger(__name__)
    self.logger.setLevel(loglevel);
    self.snsService = snsService
    self.workloadRegion = workloadRegion;
    self.ec2Resource = self.makeEC2ResourceConnection();
    self.ec2Client = self.makeEC2ClientConnection();
    self.elbClient = self.makeELBClientConnection();

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupELBs(self):
    flag = True
    try:
      elbsInRegion = self.elbClient.describe_load_balancers();
    except Exception as e:
      msg = 'Exception obtaining ELBs in region %s --> %s' % (self.workloadRegion, e)
      subject_prefix = "Scheduler Exception in %s" % self.workloadRegion
      logger.error(msg + str(e))
      flag = False

    if (flag == False):
      try:
        self.snsService.sendSns("lookupELBs() has encountered an exception ", str(e));
      except Exception as e:
        logger.error('Exception publishing SNS message %s' % str(e))

    return(elbsInRegion)

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupInstancesByFilter(self, workloadSpecificationDict, tierName, targetInstanceStateString):
    # Use the filter() method of the instances collection to retrieve
    # all running EC2 instances.
    self.logger.debug('lookupInstancesByFilter() seeking instances in tier %s' % tierName)
    self.logger.debug(
      'lookupInstancesByFilter() instance state %s' % targetInstanceStateString
    )
    self.logger.debug(
      'lookupInstancesByFilter() tier tag key %s' % workloadSpecificationDict[ComputeServices.TIER_FILTER_TAG_KEY]
    )
    self.logger.debug('lookupInstancesByFilter() tier tag value %s' % tierName)
    self.logger.debug('lookupInstancesByFilter() Env tag key %s' % workloadSpecificationDict[
      ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY]
                      )
    self.logger.debug('lookupInstancesByFilter() Env tag value %s' % workloadSpecificationDict[
      ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE]
                      )

    targetFilter = [
      {
        'Name': 'instance-state-name',
        'Values': [targetInstanceStateString]
      },
      {
        'Name': 'tag:' + workloadSpecificationDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY],
        'Values': [workloadSpecificationDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE]]
      },
      {
        'Name': 'tag:' + workloadSpecificationDict[ComputeServices.TIER_FILTER_TAG_KEY],
        'Values': [tierName]
      }
    ]

    # If the Optional VPC ID was provided to further tighten the filter, include it.
    # Only instances within the specified region and VPC within region are returned
    if (ComputeServices.WORKLOAD_VPC_ID_KEY in workloadSpecificationDict):
      vpc_filter_dict_element = {
        'Name': 'vpc-id',
        'Values': [workloadSpecificationDict[ComputeServices.WORKLOAD_VPC_ID_KEY]]
      }
      targetFilter.append(vpc_filter_dict_element)
      self.logger.debug('VPC_ID provided, Filter List is %s' % str(targetFilter))

    # Filter the instances
    targetInstanceColl = {}
    try:
      targetInstanceColl = sorted(self.ec2Resource.instances.filter(Filters=targetFilter))
      self.logger.info('lookupInstancesByFilter(): # of instances found for tier %s in state %s is %i' % (
        tierName, targetInstanceStateString, len(list(targetInstanceColl))))
      if (self.logger.getEffectiveLevel() == logging.DEBUG):
        for curr in targetInstanceColl:
          self.logger.debug('lookupInstancesByFilter(): Found the following matching targets %s' % curr)
    except Exception as e:
      msg = 'lookupInstancesByFilter() Exception encountered during instance filtering %s -->' % e
      self.logger.error(msg + str(e))
      raise e

    return targetInstanceColl

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Connection Factory
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeEC2ResourceConnection(self):
    try:
      self.logger.debug('obtaining boto3 ec2 resource ');
      self.ec2Resource = boto3.resource('ec2', region_name=self.workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 ec2 resource in region %s -->' % self.workloadRegion
      self.logger.error(msg + str(e));
    return (self.ec2Resource);

  def getEC2ResourceConnection(self):
    if (self.ec2Resource):
      return (self.ec2Resource)
    else:
      return (self.makeEC2ResourceConnection());

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeEC2ClientConnection(self):
    try:
      self.logger.debug('obtaining boto3 ec2 client ');
      self.ec2Client = boto3.client('ec2', region_name=self.workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 ec2 client in region %s -->' % self.workloadRegion
      self.logger.error(msg + str(e));
    return (self.ec2Client);

  def getEC2ClientConnection(self):
    if (self.ec2Client):
      return (self.ec2Client)
    else:
      return (self.makeECClientConnection());

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeELBClientConnection(self):
    try:
      self.logger.debug('obtaining boto3 elb client ');
      self.elbClient = boto3.client('elb', region_name=self.workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 elb client in region %s -->' % self.workloadRegion
      self.logger.error(msg + str(e));
    return (self.elbClient);

  def getELBClientConnection(self):
    if (self.elbClient):
      return (self.elbClient)
    else:
      return (self.makeELBClientConnection());

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Testing
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == "__main__":
  # setup logging service
  logLevel = logging.INFO
  region = 'us-west-2'

  notificationService = NotificationServices('SchedulerTesting', 'TestWorkloadName', region, logLevel);

  compService = ComputeServices(notificationService, region, logLevel);

  # Test ELBs
  elbsInRegion = compService.lookupELBs();
  print('ELBs found in region %s are %s' % (region, json.dumps(elbsInRegion, indent=4)));

  # Test Instances
  workloadDict = {
    ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY : 'Application',
    ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE : 'Crypto',
    ComputeServices.TIER_FILTER_TAG_KEY : 'ApplicationRole',
  }
  matchedCollectionOfInstances = compService.lookupInstancesByFilter(workloadDict, 'NAT', ComputeServices.BOTO3_INSTANCE_STATE_MAP[16]);
  print('Matched Instances %s' % json.dumps(str(matchedCollectionOfInstances), indent=4));