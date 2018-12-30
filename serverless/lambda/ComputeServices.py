import logging
import boto3
import json
from botocore.exceptions import ClientError

from redo import retriable, retry  # See action function  https://github.com/mozilla-releng/redo
#from retrying import retry  # See  https://pypi.org/project/retrying/ seems unable to natively wrap api calls

# CloudWatch Logs
#import watchtower

from NotificationServices import NotificationServices
from LoggingServices import makeLogger



class ComputeServices(object):
  WORKLOAD_SPEC_REGION_KEY = 'WorkloadRegion'

  WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY = 'WorkloadFilterTagName'
  WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE = 'WorkloadFilterTagValue'

  WORKLOAD_VPC_ID_KEY = 'VPC_ID'

  TIER_FILTER_TAG_KEY = 'TierFilterTagName'
  TIER_FILTER_TAG_VALUE = 'TierTagValue'

  ACTION_START = 'Start'
  ACTION_STOP = 'Stop'

  TIER_STOP = 'TierStop'
  TIER_START = 'TierStart'
  TIER_SCALING = 'TierScaling'
  TIER_SEQ_NBR = 'TierSequence'



  # These are the official codes per Boto3
  BOTO3_INSTANCE_STATE_MAP = {
    0: 'pending',
    16: 'running',
    32: 'shutting-down',
    48: 'terminated',
    64: 'stopping',
    80: 'stopped'
  }


  def __init__(self, logLevelStr):
    '''
    Invoke outside of lambda_handler fcn.  That is, invoke once.
    This allows for the api based resources to be created once per cold start
    '''
    self.logger = makeLogger(__name__, logLevelStr);
    self.ec2Resource = None
    self.ec2Client =   None
    self.elbClient =   None
    self.ec2ResourceMap = {}
    self.ec2ClientMap = {}
    self.elbClientMap = {}


  def initializeRequestState(self, dataServices, snsServices, workloadRegion):
    '''
    Invoke each time lambda_handler is called, as these may change per lambda invocation
    '''
    self.snsServices = snsServices
    self.dataServices = dataServices
    self.workloadRegion = workloadRegion;
    self.ec2Resource = self.getEC2ResourceConnection(workloadRegion);
    self.ec2Client =   self.getEC2ClientConnection(workloadRegion);
    self.elbClient =   self.getELBClientConnection(workloadRegion);

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Actions
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  def actionStopWorkload(self, workloadName, dryRunFlag):
    # Need the workload spec to correctly scope the search for instances
    workloadSpec = self.dataServices.lookupWorkloadSpecification(workloadName);

    # Sequence the Tiers within the workload
    sequencedTiersList = self.getSequencedTierNames(workloadName, ComputeServices.ACTION_STOP);

    # Iterate over the Sequenced Tiers of the workload to stop the running instances
    for currTierName in sequencedTiersList:
      self.logger.info('Stopping Tier: {}'.format(currTierName));

      # For each tier, get the Instance State of each instance
      tierInstancesByInstanceStateDict = self.getTierInstancesByInstanceState(workloadSpec, currTierName)

      # Grab the Running List within the Map
      running = self.BOTO3_INSTANCE_STATE_MAP[16]
      instancesToStop =  tierInstancesByInstanceStateDict[running]

      # Stop each instance in the list
      for currRunningInstance in instancesToStop:

        result = 'Instance not Stopped'

        if( dryRunFlag ):
          self.logger.warning('DryRun Flag is set - instance will not be stopped')
          continue;

        try:
          result = retry(currRunningInstance.stop, attempts=5, sleeptime=0, jitter=0)
          self.logger.debug('Succesfully stopped EC2 instance {}'.format(currRunningInstance.id))
          #logger.info('stopInstance() for ' + self.instance.id + ' result is %s' % result)
        except Exception as e:
          msg = 'ComputeServices.actionStopWorkload() Exception on instance {}, error {}'.format(currRunningInstance, str(e))
          self.logger.warning(msg);
          self.snsServices.sendSns('ComputeServices.actionStopWorkload()', msg);



  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Supporting Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  def getTierInstancesByInstanceState(self, workloadSpecDict, tierName):
    # Return the running and stopped instances through a dictoionary of Lists (running, stopped)
    tierInstancesByStatesDict = {}


    # Find the stopped instances of this tier
    try:
      stopped = self.BOTO3_INSTANCE_STATE_MAP[80]
      stoppedInstancesList = self.lookupInstancesByFilter(workloadSpecDict, tierName, stopped)
      tierInstancesByStatesDict[stopped] =  stoppedInstancesList
    except Exception as e:
      self.logger.error('getTierInstancesByInstanceState() for stopped had exception of {}'.format(e))
      self.snsServices.sendSns("getTierInstancesByInstanceState() has encountered an exception", str(e))

    # Find the running instances of this tier
    try:
      running = self.BOTO3_INSTANCE_STATE_MAP[16]
      runningInstancesList = self.lookupInstancesByFilter(workloadSpecDict, tierName, running)
      tierInstancesByStatesDict[running] =  runningInstancesList
    except Exception as e:
      self.logger.error('getTierInstancesByInstanceState() for running had exception of {}'.format(e))
      self.snsServices.sendSns("getTierInstancesByInstanceState() has encountered an exception", str(e))

    return(tierInstancesByStatesDict);

  def getSequencedTierNames(self, workloadName, action):
    # get the tiers
    tierSpecsDict = self.dataServices.lookupTierSpecs(workloadName);

    # Prefill list for easy insertion
    #sequencedTierNameList = range(len(tierSpecsDict))
    length = len(tierSpecsDict);
    #sequencedTierNameList = list(0) * length;
    sequencedTierNameList = list(0 for i in range(length));
    #l = list(0 for i in range(len(a)))

    # action indicates whether it is a TIER_STOP, or TIER_START, as they may have different sequences
    # Sequence is ascending
    for tierName, tierAttributes in tierSpecsDict.items():
      self.logger.debug('sequenceTiers() Action={}, currKey={}, currAttributes={})'.format(action, tierName, tierAttributes))

      # Grab the Tier Name of this item first
      #tierName = currKey

      # tierName = currAttributes[Orchestrator.TIER_NAME]

      # This will be used to point to relevant dict within a specific tier's spec dict
      tierActionAttributes = {}

      if (action == ComputeServices.ACTION_STOP):
        # Locate the TIER_STOP Dictionary
        tierActionAttributes = tierAttributes[ComputeServices.TIER_STOP]

      elif (action == Orchestrator.TIER_START):
        tierActionAttributes = tierAttributes[ComputeServices.TIER_START]

      # logger.info('In sequenceTiers(): tierAttributes is ', tierAttributes )

      # Insert into the List
      idx = int(tierActionAttributes[ComputeServices.TIER_SEQ_NBR]);
      #sequencedTierNameList.insert(int(insertIdx), tierName);
      sequencedTierNameList[idx] = tierName;

    self.logger.debug('sequenceTiers() List for Action={} is {}'.format(action, sequencedTierNameList))

    return (sequencedTierNameList)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Lookups
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupELBs(self):
    flag = True
    try:
      elbsInRegion = self.elbClient.describe_load_balancers();
    except Exception as e:
      msg = 'Exception obtaining ELBs in region %s --> %s' % (workloadRegion, e)
      subject_prefix = "Scheduler Exception in %s" % workloadRegion
      self.logger.error(msg + str(e))
      flag = False

    if (flag == False):
      try:
        self.snsService.sendSns("lookupELBs() has encountered an exception ", str(e));
      except Exception as e:
        self.logger.error('Exception publishing SNS message %s' % str(e))

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

    self.logger.debug('Filter is {}'.format(targetFilter))

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
      targetInstanceColl = self.ec2Resource.instances.filter(Filters=targetFilter)
#      targetInstanceColl = sorted(self.ec2Resource.instances.filter(Filters=targetFilter))
      self.logger.info('lookupInstancesByFilter(): # of instances found for tier %s in state %s is %i' % (
        tierName, targetInstanceStateString, len(list(targetInstanceColl))))
      if (self.logger.getEffectiveLevel() == logging.DEBUG):
        for curr in targetInstanceColl:
          self.logger.debug('lookupInstancesByFilter(): Found the following matching targets %s' % curr)
    except Exception as e:
      msg = 'lookupInstancesByFilter() Exception encountered during instance filtering '
      self.logger.error(msg + str(e))
      raise e

    return targetInstanceColl

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Connection Factory
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeEC2ResourceConnection(self, workloadRegion):
    try:
      self.logger.debug('obtaining boto3 ec2 resource ');
      self.ec2Resource = boto3.resource('ec2', region_name=workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 ec2 resource in region %s -->' % workloadRegion
      self.logger.error(msg + str(e));
    return (self.ec2Resource);

  def getEC2ResourceConnection(self, workloadRegion):
    if (workloadRegion not in self.ec2ResourceMap):
      self.ec2ResourceMap[workloadRegion] = self.makeEC2ResourceConnection(workloadRegion);
      self.logger.info('Added {} based boto3 ec2 resource'.format(workloadRegion));

    return (self.ec2ResourceMap[workloadRegion])


  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeEC2ClientConnection(self, workloadRegion):
    try:
      self.logger.debug('obtaining boto3 ec2 client ');
      self.ec2Client = boto3.client('ec2', region_name=workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 ec2 client in region %s -->' % workloadRegion
      self.logger.error(msg + str(e));
    return (self.ec2Client);

  def getEC2ClientConnection(self, workloadRegion):
    if (workloadRegion not in self.ec2ClientMap):
      self.ec2ClientMap[workloadRegion] = self.makeEC2ClientConnection(workloadRegion);
      self.logger.info('Added {} based boto3 ec2 client'.format(workloadRegion));

    return (self.ec2ClientMap[workloadRegion])

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeELBClientConnection(self, workloadRegion):
    try:
      self.logger.debug('obtaining boto3 elb client ');
      self.elbClient = boto3.client('elb', region_name=workloadRegion);
    except Exception as e:
      msg = 'Exception obtaining botot3 elb client in region %s -->' % self.workloadRegion
      self.logger.error(msg + str(e));
    return (self.elbClient);

  def getELBClientConnection(self, workloadRegion):
    if (workloadRegion not in self.elbClientMap):
      self.elbClientMap[workloadRegion] = self.makeELBClientConnection(workloadRegion);
      self.logger.info('Added {} based boto3 elb client'.format(workloadRegion));

    return (self.elbClientMap[workloadRegion])

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Testing
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == "__main__":
  # setup logging service
  logLevel = logging.INFO
  region = 'us-west-2'

  topic='SchedulerTesting'
  workloadName='TestWorkloadName'
  notificationService = NotificationServices(logLevel);
  notificationService.initializeRequestState(topic, workloadName, region);


  compService = ComputeServices(logLevel);
  compService.initializeRequestState(notificationService, workloadName, region);

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