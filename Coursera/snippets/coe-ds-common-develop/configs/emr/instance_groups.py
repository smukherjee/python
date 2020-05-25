instance_groups = [
  {
    "InstanceCount": 1,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 192,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 1
        }
      ]
    },
    "InstanceGroupType": "CORE",
    "InstanceType": "r5.4xlarge",
    "Name": "Core Instance Group"
  },
  {
    "InstanceCount": 1,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 192,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 1
        }
      ]
    },
    "InstanceGroupType": "MASTER",
    "InstanceType": "r5.4xlarge",
    "Name": "Master Instance Group"
  }
]