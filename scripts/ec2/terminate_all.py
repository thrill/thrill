#!/usr/bin/env python
##########################################################################
# scripts/ec2/terminate_all.py
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
from subprocess import call

ec2 = boto3.resource('ec2')

filters = [{'Name': 'instance-state-name', 'Values': ['running']}]
if "EC2_KEY_NAME" in os.environ:
    filters.append({'Name': 'key-name', 'Values': [os.environ['EC2_KEY_NAME']]})

instances = ec2.instances.filter(Filters=filters)

ids = [instance.id for instance in instances]
print("Terminating:", ids)

ec2.instances.filter(InstanceIds=ids).terminate()

##########################################################################
