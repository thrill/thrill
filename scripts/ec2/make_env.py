#!/usr/bin/env python

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
