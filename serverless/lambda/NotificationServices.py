import boto3
import json

from redo import retriable  # https://github.com/mozilla-releng/redo
from LoggingServices import makeLogger


class NotificationServices(object):

  '''
  Invoke outside of lambda_handler fcn.  That is, invoke once.
  This allows for the api based resources to be created once per cold start
  '''
  def __init__(self, logLevelStr):
    self.logger = makeLogger(__name__, logLevelStr);
    self.snsResource = None;
    self.snsMap = {};  # form is { region: botoObject, region: botoObject, ... ]


  def initializeRequestState(self, topic, workload, region):
    '''
    Invoke each time lambda_handler is called, as these may change per lambda invocation
    '''
    self.topic = topic
    self.workload = workload
    self.region = region
    self.snsResource = self.getSNSResource(region);



  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeSNSResource(self, region):
    try:
      self.logger.debug('obtaining boto3 sns resource ');
      self.snsResource = boto3.resource('sns', region_name=region);
    except Exception as e:
      msg = 'Exception obtaining botot3 sns resource in region %s -->' % region
      self.logger.error(msg + str(e));
    return (self.snsResource);

  def getSNSResource(self, region):
    if(region not in self.snsMap):
      self.snsMap[region] = self.makeSNSResource(region);
      self.logger.info('Added {} based boto3 sns client'.format(region));

    return(self.snsMap[region])

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def sendSns(self, subject, message):
    try:
      topic = self.snsResource.create_topic(Name=self.topic)  # This action is idempotent.
      topic.publish(Subject=subject, Message=str(" Workload : " + self.workload + '\n') + str(" Exception : " + message))
    except Exception as e:
      self.logger.error('Error sending SNS message.  Exception is %s' % str(e) )

if __name__ == "__main__":
  # setup logging service
  logLevel = logging.INFO
  topic = 'SchedulerTesting'

  # setup data service
  notificationService = NotificationServices(logLevel);
  notificationService.initializeRequestState(topic, 'TestWorkloadName', 'us-west-2');
  notificationService.sendSns('Test Subject', 'Test Exception Message')

  # simulate new lambda request same region
  notificationService.initializeRequestState(topic, 'TestWorkloadName', 'us-west-2');
  notificationService.sendSns('Test Subject', 'Test Exception Message')

  # simulate new lambda request different region
  notificationService.initializeRequestState(topic, 'TestWorkloadName', 'us-east-1');
  notificationService.sendSns('Test Subject', 'Test Exception Message')
