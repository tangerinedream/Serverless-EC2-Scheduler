import boto3
import json
import time

from botocore.exceptions import ClientError
from redo import retriable, retry  # See action function  https://github.com/mozilla-releng/redo
#from retrying import retry  # See  https://pypi.org/project/retrying/ seems unable to natively wrap api calls
import re

import WorkloadConstants
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

  REGISTER_TO_ELB = 'Register'
  UNREGISTER_FROM_ELB = 'Unregister'

  BOTO3_INSTANCE_STATE_STOPPED = 'stopped'
  BOTO3_INSTANCE_STATE_RUNNING = 'running'

  T_UNLIMITED_REGEX_EXPRESSION='[u,U]'



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

    # Get this once per request
    self.elbsInRegionList = self.getELBListInRegion();

    self.vpcId = None;  # This will be captured/set in lookupInstancesByFilter()

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Workload Actions
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  def actionStartWorkload(self, workloadName, dryRunFlag, profileName=None):
    # Need the workload spec to correctly scope the search for instances
    workloadSpec = self.dataServices.lookupWorkloadSpecification(workloadName);

    # Sequence the Tiers within the workload
    sequencedTiersList = self.dataServices.getSequencedTierNames(workloadName, WorkloadConstants.ACTION_START);

    instancesStarted = [];


    # Iterate over the Sequenced Tiers of the workload to start the stopped instances
    for currTierName in sequencedTiersList:
      self.logger.info('Starting Tier: {}'.format(currTierName));

      totalInstancesInTier = self.getTierInstances( workloadSpec, currTierName )

      # Scale all Instances in the tier if Profile Provided
      if(profileName is not None):
        targetInstanceTypeForTier = self.dataServices.getTargetInstanceTypeForTierProfile( currTierName, profileName );
        # If profile exists and contains an instance type, attempt to scale the tier
        if( targetInstanceTypeForTier ):
          for currInstance in totalInstancesInTier:
            self.scaleInstance(currInstance, targetInstanceTypeForTier)

      toStartInstanceList = self.makeListOfInstancesToStart( currTierName, totalInstancesInTier, profileName );

      # Start each instance in the list
      for currInstance in toStartInstanceList:
        result = 'Instance not Running'

        if( dryRunFlag ):
          self.logger.warning('DryRun Flag is set - instance will not be started')
          continue;

        # Address reregistration of instance to ELB(s) if appropriate
        self.reregisterInstanceToELBs(currInstance);

        try:
          self.logger.info( 'Attempting to Start {}'.format( currInstance.id ) )
          result = retry(currInstance.start, attempts=5, sleeptime=0, jitter=0);
          instancesStarted.append(currInstance.id);

          self.logger.info('Successfully started EC2 instance {}'.format(currInstance.id))

        except Exception as e:
          msg = 'ComputeServices.actionStartWorkload() Exception on instance {}, error {}'.format(currInstance, str(e))
          self.logger.warning(msg);
          self.snsServices.sendSns('ComputeServices.actionStartWorkload()', msg);

      # InterTier Orchestration Delay
      sleepValue = self.dataServices.getInterTierOrchestrationDelay(currTierName, WorkloadConstants.ACTION_START);
      time.sleep(sleepValue);

    return(instancesStarted)


  def actionStopWorkload(self, workloadName, dryRunFlag):
    # Need the workload spec to correctly scope the search for instances
    workloadSpec = self.dataServices.lookupWorkloadSpecification(workloadName);

    # Sequence the Tiers within the workload
    sequencedTiersList = self.dataServices.getSequencedTierNames(workloadName, WorkloadConstants.ACTION_STOP);

    instancesStopped = [];

    # Iterate over the Sequenced Tiers of the workload to stop the running instances
    for currTierName in sequencedTiersList:
      self.logger.info('Stopping Tier: {}'.format(currTierName));

      # Get the instances, and pull out those that need to be stopped
      totalTierInstanceList = []
      totalInstancesInTier = self.getTierInstances(workloadSpec, currTierName)
      for i in totalInstancesInTier:
        totalTierInstanceList.append( i )  # from Collection --> List

      # Get the Running instances from the total list
      instancesToStopList = self.getTierInstancesByInstanceState(totalTierInstanceList, self.BOTO3_INSTANCE_STATE_RUNNING)
      if(len(instancesToStopList) == 0):
        self.logger.info( 'No instances found to Stop')
      else:
        for instance in instancesToStopList:
          self.logger.info('Instance found to Stop {}'.format(instance.id))


      # instancesToStop = self.getTierInstancesByInstanceState(totalTierInstanceList, self.BOTO3_INSTANCE_STATE_RUNNING)

      # Stop each instance in the list
      for currRunningInstance in instancesToStopList:
        result = 'Instance not Stopped'

        if( dryRunFlag ):
          self.logger.warning('DryRun Flag is set - instance will not be stopped')
          continue;

        try:
          self.logger.info( 'Attempting to Stop {}'.format(currRunningInstance.id) )
          result = retry(currRunningInstance.stop, attempts=5, sleeptime=0, jitter=0);
          instancesStopped.append(currRunningInstance.id);

          self.logger.info('Successfully stopped EC2 instance {}'.format(currRunningInstance.id))
          #logger.info('stopInstance() for ' + self.instance.id + ' result is %s' % result)

        except Exception as e:
          msg = 'ComputeServices.actionStopWorkload() Exception on instance {}, error {}'.format(currRunningInstance, str(e))
          self.logger.warning(msg);
          self.snsServices.sendSns('ComputeServices.actionStopWorkload()', msg);

      # InterTier Orchestration Delay
      sleepValue = self.dataServices.getInterTierOrchestrationDelay( currTierName, WorkloadConstants.ACTION_STOP );
      time.sleep( sleepValue );

    return(instancesStopped)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Scaling Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  def scaleInstance( self, instance, targetInstanceType ):

    result = 'no result'
    instanceState = instance.state
    if (instanceState['Name'] == 'stopped'):

      # Tokenize instance type
      tokenizedTargetInstanceType = targetInstanceType.split( '.' )
      targetInstanceFamily = tokenizedTargetInstanceType[0]
      targetInstanceType = tokenizedTargetInstanceType[0] + '.' + tokenizedTargetInstanceType[1]

      # "t" class instances to be forced to EbsOptimized == False, and checked for Unlimited setting
      tTypeInstanceFamilyList = ['t2', 't3']
      if (targetInstanceFamily in tTypeInstanceFamilyList):
        # If "t" instance family (e.g. t2 or t3),
        #   1. Set EBS Optimized to False
        #   2. Check to see if we need to flip from/to "standard" / "unlimited"
        ebsOptimizedAttr = False
        uFlag = False
        if( len(tokenizedTargetInstanceType) == 3 ):
          targetUnlimitedFlag =  tokenizedTargetInstanceType[2]

          # Does it have unlimited flag set ?
          uFlag=re.search(ComputeServices.T_UNLIMITED_REGEX_EXPRESSION, targetUnlimitedFlag);

        self.setTFamilyInstanceCreditSpecification(instance, uFlag)

      else:
        ebsOptimizedAttr = instance.ebs_optimized  # May have been set to True or False previously

      # Now that t2/t3 unlimited flag is correctly set,
      # we can finally change instance type and/or size as well as set the ebsOptimize flag

      self.executeChangeInstanceType(instance, targetInstanceType);

      self.executeModifyEBSOptimizationAttr(instance, ebsOptimizedAttr);

      self.validateInstanceChanges(instance, targetInstanceType);

    else:
      logMsg = (
        'scaleInstance() requested to change instance type for non-stopped instance {}. '
        'No action taken'.format(instance.id)
      )
      self.logger.warning( logMsg )

  def executeChangeInstanceType( self, instance, targetInstanceType ):
    modifiedInstanceTypeKwargs = {
      "InstanceType": {'Value': targetInstanceType}
    }

    # Boto3 only allows you to modify one attribute at a time.
    try:
      result = retry( instance.modify_attribute, attempts=5, jitter=0, sleeptime=0, kwargs=modifiedInstanceTypeKwargs )
      self.logger.info( 'Instance {} scaled to Instance Type {}'.format(
        instance.id,
        targetInstanceType )
      )
    except Exception as e:
      msg = 'scaleInstance() Exception for instance.modify_attribute(). Instance {} , error --> {}'.format(
        instance.id,
        str( e )
      )
      self.logger.warning( msg )
      self.snsServices.sendSns(
        "scaleInstance():instance.modify_attribute() for instance change has encountered an exception", str( e ) )

  def executeModifyEBSOptimizationAttr( self, instance, ebsOptimizedAttr ):
    # Boto3 only allows you to modify one attribute at a time.
    modifiedEBSOptimizedKwargs = {
      "EbsOptimized": {'Value': ebsOptimizedAttr}
    }
    try:
      result = retry( instance.modify_attribute, attempts=5, jitter=0, sleeptime=0, kwargs=modifiedEBSOptimizedKwargs )
    except Exception as e:
      msg = 'scaleInstance() Exception for instance.modify_attribute(). Instance {} , error --> {}'.format(
        instance.id,
        str( e )
      )
      self.logger.warning( msg )
      self.snsServices.sendSns(
        "scaleInstance():instance.modify_attribute() for EBSOptimization has encountered an exception", str( e ) )

  def validateInstanceChanges( self, instance, targetInstanceType ):
    try:
      result = retry(
        self.compareInstanceTypeValues,
        attempts=8, sleeptime=15, jitter=0,
        args=(instance, targetInstanceType,)
      )
      self.logger.debug(
        'scaleInstance():compareInstanceTypeValues for instance {}, result is {}'.format( instance.id, result ) )

    except Exception as e:
      msg = ('scaleInstance() Exception for instance {}, error --> currentInstanceTypeValue'
             'does not match targetInstanceType'.format( instance.id )
             )
      self.logger.warning( msg )
      tagsYaml = self.retrieveTags( instance )
      snsSubject = 'scaleInstance() has encountered an exception for instance {}'.format( instance.id )
      snsMessage = '%s\n%s' % (str( e ), str( tagsYaml ))
      self.snsServices.sendSns( snsSubject, snsMessage )

  def setTFamilyInstanceCreditSpecification( self, instance, unlimitedFlag ):

    # Get the instance's current Credit State
    instanceCreditState = None
    instanceIdKswargs = {
      "InstanceIds": [instance.id]
    }
    try:
      result = retry(
        self.ec2Client.describe_instance_credit_specifications,
        attempts=5, jitter=0, sleeptime=0,
        kwargs=instanceIdKswargs
      )
      instanceCreditState = result['InstanceCreditSpecifications'][0]['CpuCredits']
    except Exception as e:
      msg = 'setTFamilyInstanceCreditSpecification() exception on describe_instance_credit_specifications() for EC2 instance {}, error --> {}'.format(
        instance.id,
        str( e )
      )
      self.logger.warning( msg )
      raise e

    try:
      # Check current and if standard change instance_credit_specification to unlimited
      if(instanceCreditState == 'standard' and unlimitedFlag==True):
        # Change credit state to unlimited
        result = retry(
          self.ec2Client.modify_instance_credit_specification,
          attempts=5, sleeptime=0, jitter=0,
          kwargs={"InstanceCreditSpecifications": [{'InstanceId': instance.id, 'CpuCredits': 'unlimited'}]}
        )

      # Check current and if unlimited change instance_credit_specification to standard
      elif(instanceCreditState == 'unlimited' and unlimitedFlag==False):
        # Change credit state to standard
        result = retry(
          self.ec2Client.modify_instance_credit_specification,
          attempts=5, sleeptime=0, jitter=0,
          kwargs={"InstanceCreditSpecifications": [{'InstanceId': instance.id, 'CpuCredits': 'standard'}]}
        )

    except Exception as e:
      msg = ('setTFamilyInstanceCreditSpecification() exception on modify_instance_credit_specification()'
      'for EC2 instance {}, error --> {}'.format(
        instance.id,
        str( e )
      ))
      self.logger.warning( msg )
      raise e

  def compareInstanceTypeValues( self, instance, requestedInstanceTypeValue ):
    currentInstanceTypeValue = list(
      self.ec2Client.describe_instance_attribute( InstanceId=instance.id, Attribute='instanceType' )[
        'InstanceType'].values()
    )

    self.logger.debug( 'compareInstanceTypeValues(): Instance-> {} Current Type-> {} Target Type-> {}'.format(
        instance.id,
        currentInstanceTypeValue,
        requestedInstanceTypeValue
      )
    )

    if (currentInstanceTypeValue[0] not in requestedInstanceTypeValue):
      raise ValueError( "Current instance type value does not match modified instance type value." )

  def retrieveTags( self, instance ):
    ##Return EC2 Tags of given instance in YAML format.
    tags = self.ec2Client.describe_instances(InstanceIds=[instance.id] )['Reservations'][0]['Instances'][0]['Tags']
    tagsYaml = yaml.safe_dump( tags, explicit_start=True, default_flow_style=False )
    logger.info( 'retrieveTags(): Retrieve and return instance EC2 tags in YAML format.' )
    logger.debug( 'retrieveTags(): EC2 tag details for {}'.format(instance.id) )
    logger.debug( 'retrieveTags(): {}'.format(tagsYaml) )
    return tagsYaml

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Supporting Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  def makeListOfInstancesToStart( self, currTierName, tierInstancesCollection, profileName ):

    # There's no method to get the number of elements in this collection, only iterate through them
    tierInstancesList = []
    for i in tierInstancesCollection:
      tierInstancesList.append( i )  # from Collection --> List

    instanceCountToStartCnt = self.dataServices.calculateFleetSubset( currTierName, len(tierInstancesList), profileName );

    # Now pick the instances to start from the fleet subset
    instancesToStartList = tierInstancesList[:instanceCountToStartCnt]

    for excludedInstance in tierInstancesList[instanceCountToStartCnt:] :
      self.logger.info('Instance {} excluded from instance start list due to Fleet Subset specification.'.format(excludedInstance.id))

    # Remove instances that are already in running state
    for instanceToRemove in instancesToStartList:
      if (instanceToRemove.state['Name'] == ComputeServices.BOTO3_INSTANCE_STATE_RUNNING):
        instancesToStartList.remove( instanceToRemove )
        self.logger.info( 'Instance {} excluded from instance Start list as it is already running'.format( instanceToRemove.id ) )

    return (instancesToStartList);

  def getTierInstancesByInstanceState( self, instances, state ):
    res = []
    for i in instances:
      instanceStateDict = i.state
      if( instanceStateDict['Name'] == state):
        res.append(i)

    return(res)

  def getTierInstances(self, workloadSpecDict, tierName):
    # Return the running and stopped instances through a dictoionary of Lists (running, stopped)
    allTierInstancesDict = {}
    allInstanceStatesExceptTerminated = ['pending', 'running', 'shutting-down', 'stopping', 'stopped']

    # Find the stopped instances of this tier
    try:
      allTierInstancesDict = self.lookupInstancesByFilter(workloadSpecDict, tierName, allInstanceStatesExceptTerminated)
    except Exception as e:
      self.logger.error('getTierInstances() for stopped had exception of {}'.format(e))
      self.snsServices.sendSns("getTierInstances() has encountered an exception", str(e))

    return(allTierInstancesDict);


  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # ELB Focused Methods
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def reregisterInstanceToELBs( self, instance ):

      memberList = self.getELBMembershipListForInstance(instance);

      if( memberList ):
        # List has at least one element

        for elbName in memberList:

          self.logger.info('Instance {} is attached to ELB {}, and will be deregistered and re-registered'.format(
            instance.id,
            elbName)
          )

        self.doELBRegistrationAction(ComputeServices.UNREGISTER_FROM_ELB, instance, elbName)

        self.doELBRegistrationAction(ComputeServices.REGISTER_TO_ELB, instance, elbName)

      else:
        self.logger.debug('Instance {} is not registered behind any ELB.'.format(instance.id))

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def doELBRegistrationAction( self, action, instance, elbName ):
    try:
      if(action == ComputeServices.UNREGISTER_FROM_ELB):
        self.elb.deregister_instances_from_load_balancer(
          LoadBalancerName=elb_name,
          Instances=[{'InstanceId': instance.id}]
        )
      else:
        self.elb.register_instances_with_load_balancer(
          LoadBalancerName=elb_name,
          Instances=[{'InstanceId': instance.id}]
        )

    except Exception as e:
      logger.warning('ELB action {} encountered an exception of -->'.format(action, str(e) ));

    self.logger.debug('Succesfully addressed {} for instance {} from load balancer {}'.format(action, instance.id, elbName) )

  def getELBMembershipListForInstance( self, instance ):
    resultList = []

    for i in self.elbsInRegionList['LoadBalancerDescriptions']:
      for j in i['Instances']:
        if j['InstanceId'] == instance.id:
          elbName = i['LoadBalancerName']
          resultList.append(elbName);

    return(resultList);

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def getELBListInRegion(self):
    flag = True
    try:
      elbsInRegionList = self.elbClient.describe_load_balancers();
    except Exception as e:
      msg = 'Exception obtaining ELBs in region %s --> %s' % (self.workloadRegion, e)
      subject_prefix = "Scheduler Exception in %s" % self.workloadRegion
      self.logger.error(msg + str(e))
      flag = False

    # Flag is used to ensure retry attempts, which won't happen within except block.
    if (flag == False):
      try:
        self.snsService.sendSns("lookupELBs() has encountered an exception ", str(e));
      except Exception as e:
        self.logger.error('Exception publishing SNS message %s' % str(e))

    return(elbsInRegionList)


  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Lookups
  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  @retriable(attempts=5, sleeptime=0, jitter=0)
  def lookupInstancesByFilter(self, workloadSpecificationDict, tierName, targetInstanceStateList=None):
    # Use the filter() method of the instances collection to retrieve
    # all running EC2 instances.
    specDict = workloadSpecificationDict[WorkloadConstants.WORKLOAD_RESULTS_KEY][0]
    self.logger.debug(
      ('lookupInstancesByFilter()] '
        '[Tier Key-->{}] '
        '[Tier Value-->{}] '
        '[Environment Key-->{}] '
        '[Environment Value-->{}] '
        '[State-->{}] '.format(
          specDict[ComputeServices.TIER_FILTER_TAG_KEY],
          tierName,
          specDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY],
          specDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE],
          targetInstanceStateList
        )
      )
    )

    targetFilter = [
      {
        'Name': 'tag:' + specDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY],
        'Values': [specDict[ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE]]
      },
      {
        'Name': 'tag:' + specDict[ComputeServices.TIER_FILTER_TAG_KEY],
        'Values': [tierName]
      }
    ]

    if( targetInstanceStateList ):
      instance_state_dict_element = {
        'Name': 'instance-state-name',
        'Values': targetInstanceStateList
      }
      targetFilter.append(instance_state_dict_element)
      self.logger.debug('instance-state-name provided ->{}<-'.format(instance_state_dict_element))


    # If the Optional VPC ID was provided to further tighten the filter, include it.
    # Only instances within the specified region and VPC within region are returned
    if (ComputeServices.WORKLOAD_VPC_ID_KEY in workloadSpecificationDict):
      self.vpcId = specDict[ComputeServices.WORKLOAD_VPC_ID_KEY]
      vpc_filter_dict_element = {
        'Name': 'vpc-id',
        'Values': [self.vpcId]
      }
      targetFilter.append(vpc_filter_dict_element)
      self.logger.debug('VPC_ID provided, Filter List is {}'.format(targetFilter))

    self.logger.debug( 'Filter is {}'.format( targetFilter ) )

    # Filter the instances
    targetInstanceColl = {}
    try:
      targetInstanceColl = self.ec2Resource.instances.filter(Filters=targetFilter)
#      targetInstanceColl = sorted(self.ec2Resource.instances.filter(Filters=targetFilter))

      targetInstanceCollLen = len( list( targetInstanceColl ) )
      self.logger.debug('lookupInstancesByFilter(): {} instances found for tier {} in state {}'.format(
          targetInstanceCollLen,
          tierName,
          targetInstanceStateList
        )
      )

      # if (self.logger.getEffectiveLevel() == logging.DEBUG):
      for curr in targetInstanceColl:
        self.logger.debug('lookupInstancesByFilter(): Found the following matching targets {}'.format(curr))

    except Exception as e:
      msg = 'lookupInstancesByFilter() Exception encountered during instance filtering '
      self.logger.error(msg + str(e))
      raise e

    return(targetInstanceColl)

  # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Connection Factories
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
  print('ELBs found in region %s are %s' % (region, json.dumps(compService.elbsInRegionList, indent=4)));

  # Test Instances
  workloadDict = {
    ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_KEY : 'Application',
    ComputeServices.WORKLOAD_ENVIRONMENT_FILTER_TAG_VALUE : 'Crypto',
    ComputeServices.TIER_FILTER_TAG_KEY : 'ApplicationRole',
  }
  matchedCollectionOfInstances = compService.lookupInstancesByFilter(workloadDict, 'NAT', [ComputeServices.BOTO3_INSTANCE_STATE_RUNNING]);
  print('Matched Instances %s' % json.dumps(str(matchedCollectionOfInstances), indent=4));