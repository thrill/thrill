#!/usr/bin/env python
##########################################################################
# run/ec2-setup/terminate.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import json
import sys

from subprocess import call

with open('config.json') as data_file:
    data = json.load(data_file)

ec2 = boto3.resource('ec2')

if len(sys.argv) != 2:
    print "Usage: terminate.py 123"
    sys.exit();

job_id = str(sys.argv[1])

instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                                          {'Name': 'key-name', 'Values': [data["EC2_KEY_HANDLE"]]},
                                          {'Name': 'tag:JobId', 'Values':[job_id]}])

ids = [instance.id for instance in instances]
print("Terminating:", ids)

ec2.instances.filter(InstanceIds=ids).terminate()

##########################################################################
