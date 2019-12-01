#!/usr/bin/env bash

aws ec2 run-instances \
    --image-id ami-04c8d373e42fbb0e0 \
    --count 1 \
    --instance-type t3.micro \
    --key-name jerome-asselin \
    --region us-east-1 \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=coe-ds-helper}]' \
    --security-group-ids sg-029b91ceeaefb6476 \
    --subnet-id subnet-0754465ed54c02d90 \
    --iam-instance-profile Name=891440594040-Administrator \
    --associate-public-ip-address \
    --user-data "$(echo '
    sudo yum update -y
    sudo yum install -y git dos2unix
    sudo yum install -y java-1.8.0
    sudo yum remove -y java-1.7.0-openjdk
    conda install -y boto3 pyspark
    echo -e "PATH=\$PATH:.\nPYTHONPATH=/home/ec2-user/coe-ds-common\nexport PATH PYTHONPATH" | sudo tee /etc/profile.d/currentpath.sh
    sudo aws s3 cp s3://sita-coe-ds-prod-v1/scripts/clone_platform_aero.sh /usr/local/bin
    sudo dos2unix /usr/local/bin/clone_platform_aero.sh
    sudo chmod 755 /usr/local/bin/clone_platform_aero.sh
    mkdir bin
    echo -e "repos=<repos>\nclone_platform_aero.sh \$repos develop <cred>\ncd ~/\$repos\npython setup.py bdist_egg" > bin/clone
    echo -e "repos=<repos>\ncd ~/\$repos\ngit pull\npython setup.py bdist_egg" > bin/pull
    chmod 755 bin/*
')"

# Manually edit bin/clone with correct arguments
