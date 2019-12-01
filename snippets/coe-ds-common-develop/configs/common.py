"""
Common configurations
"""

import os
import pkg_resources
from utils.map import Map

_PROJECT_NAME = 'coe-ds-common'


def get_package_version():
    return pkg_resources.require(_PROJECT_NAME)[0].version


def get_egg_name():
    return f'{_PROJECT_NAME.replace("-", "_")}-{get_package_version()}-py3.7.egg'


PIPELINE_SCRIPT = f'/usr/local/lib/python3.6/site-packages/{get_egg_name()}/utils/pipeline.py'


class Common(Map):
    """
    Common configurations
    """
    def __init__(self, environment='dev', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url_prefix = 's3:/'
        self.bucket_prefix = 'sita-coe-ds'
        self.environment = environment
        self.version = 'v1'
        self.mart_folder = 'mart'
        self.script_folder = 'scripts'
        self.packages_folder = 'packages'
        self.bad_record_path = '/tmp/bad_records/'
        self.pipeline_name = self.__class__.__name__.lower()

    def get_bucket(self):
        return f'{self.bucket_prefix}-{self.environment}-{self.version}'

    def get_write_path(self):
        return f'{self.url_prefix}/{self.get_bucket()}/{self.mart_folder}/' \
               f'{self.pipeline_name}'

    def get_script_path(self):
        return f'{self.url_prefix}/{self.get_bucket()}/{self.script_folder}'

    def get_packages_path(self):
        return f'{self.url_prefix}/{self.get_bucket()}/{self.packages_folder}'

    def deploy_to_s3(self, run=False):
        cmds = [f'aws s3 cp $PYTHONPATH/scripts/emr_setup.sh {self.get_script_path()}/',
                f'aws s3 cp $PYTHONPATH/dist/{get_egg_name()} {self.get_packages_path()}/']
        if not run:
            return cmds
        else:
            for cmd in cmds:
                os.system(cmd)
