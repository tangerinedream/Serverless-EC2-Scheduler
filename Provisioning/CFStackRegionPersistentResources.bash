aws cloudformation create-stack \
  --stack-name ServerlessEC2SchedulerRegionStack \
  --template-body file://CFStackRegionPersistentResources.yaml \
  --no-disable-rollback --capabilities="CAPABILITY_IAM" \
  --parameters \
      ParameterKey=CodePipelineBucketName,ParameterValue=serverless-ec2-scheduler-codepipeline-bucket \
      ParameterKey=SNSTopicName,ParameterValue=ServerlessEC2Scheduler
