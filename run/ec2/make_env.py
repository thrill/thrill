#!/usr/bin/env python
##########################################################################
# run/ec2/make_env.py
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import sys
import os
import json
import sys

ec2 = boto3.resource('ec2')

with open('config.json') as data_file:
    data = json.load(data_file)

filters = [{'Name': 'instance-state-name', 'Values': ['running']}]
if 'filters' in data:
    filters += data['filters']

instances = ec2.instances.filter(Filters=filters)

for instance in instances:
    sys.stderr.write("%s pub %s priv %s\n" % (
        instance.id, instance.public_ip_address, instance.private_ip_address))

pub_ips = [instance.public_ip_address for instance in instances]
priv_ips = [instance.private_ip_address for instance in instances]

sys.stderr.write("%d instances\n" % (len(pub_ips)))

print("export THRILL_HOSTLIST=\"" + (" ".join(priv_ips)) + "\"")
print("export THRILL_SSHLIST=\"" + (" ".join(pub_ips)) + "\"")
print("# for cluster ssh: cssh " + (" ".join(pub_ips)))

##########################################################################
