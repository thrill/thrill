#!/usr/bin/env python
##########################################################################
# scripts/ec2/make_env.py
#
# Part of Project Thrill.
#
# Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import boto3
import sys
from subprocess import call

ec2 = boto3.resource('ec2')

instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

for instance in instances:
    sys.stderr.write("%s pub %s priv %s\n" % (instance.id, instance.public_ip_address, instance.private_ip_address))

pub_ips = [instance.public_ip_address for instance in instances]
priv_ips = [instance.private_ip_address for instance in instances]

sys.stderr.write("%d instances\n" % (len(pub_ips)))

print("export THRILL_HOSTLIST=\"" + (" ".join(priv_ips)) + "\"")
print("export THRILL_SSHLIST=\"" + (" ".join(pub_ips)) + "\"")
print("# cluster ssh: cssh " + (" ".join(pub_ips)))

##########################################################################
