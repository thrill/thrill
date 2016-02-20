#!/usr/bin/env python
##########################################################################
# run/ec2-setup/instant.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import time
import json

with open('config.json') as data_file:
    data = json.load(data_file)

ec2 = boto3.resource('ec2')

job_id = int(time.time())

blockMappings = [{'DeviceName': '/dev/sda1',
                 'Ebs': {
                    'VolumeSize': 8,
                    'DeleteOnTermination': True,
                    'VolumeType': 'gp2'
                 }
               }]

if data["VOL_SNAPSHOT_ID"]:
    blockMappings.append(
        {
            'DeviceName': data["DEVICE"],
            'Ebs': {
                'SnapshotId': data["VOL_SNAPSHOT_ID"],
                'DeleteOnTermination': True,
                'VolumeType': 'gp2'
            }
         })

instances = ec2.create_instances(ImageId = data["AMI_ID"],
                                 KeyName = data["EC2_KEY_HANDLE"],
                                 InstanceType = data["INSTANCE_TYPE"],
                                 SecurityGroups = [ data["SECGROUP_HANDLE"] ],
                                 MinCount = int(data["COUNT"]),
                                 MaxCount = int(data["COUNT"]),
                                 UserData = str(job_id),
                                 DisableApiTermination = False,
                                 BlockDeviceMappings=blockMappings)

# add tag to each instance
for instance in instances:
    instance.create_tags(Tags=[{'Key': 'JobId', 'Value': str(job_id)}])

loop = True
while loop:
    loop = False
    instances = ec2.instances.filter(
                Filters=[{'Name': 'key-name', 'Values': [data["EC2_KEY_HANDLE"]]},
                         {'Name': 'tag:JobId', 'Values':[str(job_id)]}])
    for instance in instances:
        if instance['state'] != 'running':
            loop = True

print str(data["COUNT"]) + " instances up and running! JobId: " + str(job_id)

##########################################################################
