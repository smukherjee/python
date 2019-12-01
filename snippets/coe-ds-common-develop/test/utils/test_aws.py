"""
Tests for AWS utilities.
"""

import unittest
from utils.aws import AWS


class TestAWS(unittest.TestCase):

    def test_emr_config(self):
        aws = AWS(core_instance_count=0, instance_type="r5.2xlarge", auto_terminate=False)
        print(aws.url_prefix)
        print(aws.get_write_path())
        print(aws.get_script_path())
        print(aws.emr_create_cluster()[2])
