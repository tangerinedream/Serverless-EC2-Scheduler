#!/bin/bash
aws cloudformation create-stack \
    --stack-name ServerlessEC2SchedulerIAM \
    --template-body file://CFStackIAMForAccount.yaml \
    --no-disable-rollback \
    --capabilities="CAPABILITY_IAM" \
    --parameters  \
        ParameterKey=PathNamespace,ParameterValue=serverless-ec2-scheduler
