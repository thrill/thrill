# Thrill on EC2 Scripting

Currently the only working method is to compile a thrill program locally, and then transmit the whole binary to each of the EC2 boxes.

This mean your computer uploads lots of copies of binary! It is a good idea to `strip exe` the program of debug info first.

## Setup

Install the AWS command line tools and boto3 python frontend.

```
pip install awscli boto3
```

Depending on Linux distro, you may have to replace `pip` with `pip3` or install the python libraries only as user with `pip install --user`.

Run `aws configure` and enter your AWS credentials.

Run `./make_env.py` to check if the boto3 library can connect to AWS.

## Launching Instances

Using aws command line tool:

```
aws ec2 run-instances --image-id ami-21745a56 \
      --key-name rsa.tb2 --instance-type t2.micro \
      --security-groups default \
      --enable-api-termination \
      --count 2
```

Replace `rsa.tb2` with your ssh key pair identifier. Replace `count 2` with the number of instances. And check image-id for the current development AMI.

Other useful commands:

```
aws ec2 describe-instances
```

## Scripts

- `terminate_all.py` terminates **all** running instances.

- `make_env.py` lists all running instances and creates `THRILL_` environment variables for running `invoke.sh`. Safe to run anytime.

- `invoke.sh` runs ssh invoke with the environment variables from `make_env.py`.

- Use `export EC2_KEY_NAME=rsa.keyname` to filter the selected instances to just those started with a specific ssh keypair. (Useful if multiple experiments are run under one ec2 account).

## Example

./invoke.sh -c ../../build/benchmarks/net/broadcast -r 100
