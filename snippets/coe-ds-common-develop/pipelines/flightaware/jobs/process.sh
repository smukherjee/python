#!/usr/bin/env bash

environment=pre-prod
dt_start="2019-10-31"
until="2019-10-01"
until="2019-10-30"

environment=prod
dt_start="2019-11-15"
until="2016-10-01"


python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name FlightAware --process-group raw \
  --dt-start $dt_start --until $until --n-rounds 1 --environment $environment --run True

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name FlightAware --process-group derived1 \
  --dt-start $dt_start --until $until --n-rounds 1 --environment $environment --run True

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name FlightAware --process-group derived2 \
  --dt-start $dt_start --until $until --n-rounds 1 --environment $environment --run True

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name FlightAware --process-group derived3 \
  --dt-start $dt_start --until $until --n-rounds 1 --environment $environment --run True

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name FlightAware --process-group derived4 \
  --dt-start $dt_start --until $until --n-rounds 1 --environment $environment --run True
