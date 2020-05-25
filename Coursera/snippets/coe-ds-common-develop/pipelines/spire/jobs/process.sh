#!/usr/bin/env bash

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name Spire --process-group raw \
  --dt-start "2019-09-30" --until "2018-11-01" --n-rounds 12 --environment prod --run True

python /home/ec2-user/coe-ds-common/scripts/launch_pipeline.py --class-name Spire --process-group raw \
  --dt-start "2019-10-01" --until "2019-10-31" --n-rounds 12 --environment prod --run True
