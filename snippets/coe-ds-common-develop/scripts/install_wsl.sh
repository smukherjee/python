#!/usr/bin/env bash

sudo apt-get update
sudo apt-get -y upgrade
sudo apt install -y awscli python3-pip
pip3 install --upgrade requests
pip3 install boto boto3 bs4

