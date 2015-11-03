#!/usr/bin/env python
##########################################################################
# scripts/ec2/submit.py
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import time
import json
import datetime
import sys

with open('config.json') as data_file:
    data = json.load(data_file)

client = boto3.client('ec2')
ec2 = boto3.resource('ec2')

job_id = int(time.time())

response = client.request_spot_instances(SpotPrice=data["SPOT_PRICE"],
                                       InstanceCount=data["COUNT"],
                                       Type=data["TYPE"],
                                       #ValidFrom=datetime.datetime(2015, 10, 11, 18, 10, 00),
                                       ValidUntil=datetime.datetime(2015, 10, 11, 19, 37, 00),
                                       #AvailabilityZoneGroup=data["ZONE"],
                                       LaunchSpecification={
                                            'ImageId' : data["AMI_ID"],
                                            'KeyName' : data["EC2_KEY_HANDLE"],
                                            'InstanceType' : data["INSTANCE_TYPE"],
                                            'SecurityGroups' : [ data["SECGROUP_HANDLE"] ],
                                            'Placement': { 'AvailabilityZone': data["ZONE"] }
                                       })

request_ids = []
for request in response['SpotInstanceRequests']:
    request_ids.append(request['SpotInstanceRequestId'])

running_instances = []
loop = True;

print "waiting for instances to get fulfilled..."
while loop:
    requests = client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
    for request in requests['SpotInstanceRequests']:
        if request['State'] in ['closed', 'cancelled', 'failed']:
            print request['SpotInstanceRequestId'] + " " + request['State']
            loop = False
            break; # TODO(ms) ensure running instances are terminated
        if 'InstanceId' in request and request['InstanceId'] not in running_instances:
           running_instances.append(request['InstanceId'])
           print request['InstanceId'] + " running..."
    if len(running_instances) == int(data["COUNT"]):
        print 'all requested instances are fulfilled'
        break;
    time.sleep(5)

if loop == False:
    print "unable to fulfill all requested instances... aborting..."
    sys.exit();

# ensure all instances are running
loop = True;
while loop:
    loop = False
    response = client.describe_instance_status(InstanceIds=running_instances, IncludeAllInstances=True)
    for status in response['InstanceStatuses']:
        if status['InstanceState']['Name'] != 'running':
            loop = True

print "all instances are running..."

# optionally, attach snapshot as volume to each instance
if data["VOL_SNAPSHOT_ID"]:
    print "attaching volumes..."
    for instance_id in running_instances:
        volume = ec2.create_volume(SnapshotId = data["VOL_SNAPSHOT_ID"], AvailabilityZone = data["ZONE"], VolumeType = data["VOLUME_TYPE"])
        while ec2.Volume(volume.id).state != "available":
            time.sleep(1)
        ec2.Instance(instance_id).attach_volume(VolumeId = volume.id, Device=data["DEVICE"])
        print data["VOL_SNAPSHOT_ID"] + " attached to " + instance_id
    print "volumes attached..."

print job_id