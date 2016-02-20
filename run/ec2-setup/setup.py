#!/usr/bin/env python
##########################################################################
# run/ec2-setup/setup.py
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

instances = ec2.create_instances(ImageId = data["AMI_ID"],
                                 KeyName = data["EC2_KEY_HANDLE"],
                                 InstanceType = data["INSTANCE_TYPE"],
                                 SecurityGroups = [ data["SECGROUP_HANDLE"] ],
                                 MinCount = int(data["COUNT"]),
                                 MaxCount = int(data["COUNT"]),
                                 UserData = str(job_id),
                                 DisableApiTermination = False)

# add tag to each instance
# for instance in instances:
#     instance.create_tags(Tags=[{'Key': 'JobId', 'Value': str(job_id)}])

# optionally, attach snapshot as volume to each instance
if data["VOL_SNAPSHOT_ID"]:
    attached = []
    while True:
        instances = ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                     {'Name': 'key-name', 'Values': [data["EC2_KEY_HANDLE"]]}
#                    {'Name': 'tag:JobId', 'Values':[str(job_id)]}
                     ])
        for instance in instances:
            if instance.id not in attached:
                volume = ec2.create_volume(SnapshotId = data["VOL_SNAPSHOT_ID"], AvailabilityZone = data["ZONE"], VolumeType = data["VOLUME_TYPE"])
                while ec2.Volume(volume.id).state != "available":
                    time.sleep(1)
                ec2.Instance(instance.id).attach_volume(VolumeId = volume.id, Device=data["DEVICE"])
                attached.append(instance.id)
        if len(attached) == int(data["COUNT"]):
            break;
        print "still waiting for some instances to run..."
        time.sleep(1)

print job_id

# TODO: uncomment tag once rights are granted
# TODO: move create volume from snapshot to create_instances to be able to set delete on termination

##########################################################################
