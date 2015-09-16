#!/usr/bin/env python

import boto3
from subprocess import call

ec2 = boto3.resource('ec2')

instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

ids = [instance.id for instance in instances]
print("Terminating:", ids)

ec2.instances.filter(InstanceIds=ids).terminate()
