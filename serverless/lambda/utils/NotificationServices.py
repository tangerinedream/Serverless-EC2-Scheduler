import logging
import boto3
import json

from redo import retriable  # https://github.com/mozilla-releng/redo


class NotificationServices(object):

  def __init__(self, topic, workload, region, logLevel):
    self.logger = logging.getLogger(__name__)
    self.logger.setLevel(logLevel);
    self.topic = topic
    self.workload = workload
    self.region = region
    self.snsResource = None

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def makeSNSResource(self):
    try:
      self.logger.debug('obtaining boto3 sns resource ');
      self.snsResource = boto3.resource('sns', region_name=self.region);
    except Exception as e:
      msg = 'Exception obtaining botot3 sns resource in region %s -->' % self.region
      self.logger.error(msg + str(e));
    return (self.snsResource);

  def getSNSResource(self):
    if (self.snsResource):
      return (self.snsResource)
    else:
      return (self.makeSNSResource());

  @retriable(attempts=5, sleeptime=0, jitter=0)
  def sendSns(self, subject, message):
    try:
      sns = self.getSNSResource();
      topic = sns.create_topic(Name=self.topic)  # This action is idempotent.
      topic.publish(Subject=subject, Message=str(" Workload : " + self.workload + '\n') + str(" Exception : " + message))
    except Exception as e:
      self.logger.error('Error sending SNS message.  Exception is %s' % str(e) )

if __name__ == "__main__":
  # setup logging service
  logLevel = logging.INFO
  topic = 'SchedulerTesting'

  # setup data service
  notificationService = NotificationServices(topic, 'TestWorkloadName', 'us-west-2', logLevel);
  notificationService.sendSns('Test Subject', 'Test Exception Message')
