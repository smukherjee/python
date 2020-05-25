"""
Tests for configs
"""

import unittest
from configs.common import Common, get_package_version
from pipelines.dummy.dummy_pipeline import DummyPipeline


class TestConfigs(unittest.TestCase):

    def test_config(self):
        pipeline = DummyPipeline()
        pipeline.environment = 'pre-prod'
        self.assertEqual(pipeline.get_bucket(), 'sita-coe-ds-pre-prod-v1')
        self.assertEqual(pipeline.get_write_path(), 's3://sita-coe-ds-pre-prod-v1/mart/dummypipeline')

    def test_common(self):
        common = Common()
        self.assertEqual(common.deploy_to_s3(),
                         [f'aws s3 cp scripts/emr_setup.sh s3://sita-coe-ds-dev-v1/scripts/',
                          f'aws s3 cp dist/coe_ds_common-{get_package_version()}-py3.7.egg '
                          f's3://sita-coe-ds-dev-v1/packages/'])
