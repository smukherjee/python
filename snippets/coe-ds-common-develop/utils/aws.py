"""
Utilities for AWS operations
"""
import argparse
import getpass
import importlib
import json
import os
import re
from math import ceil
from configs.common import Common, get_egg_name, PIPELINE_SCRIPT
from configs.emr import instance_groups, emr_config, ec2_attributes
from utils.date import date_range
from utils.pipeline import Pipeline


class AWS(Common):
    """
    AWS operations
    """

    def __init__(self, core_instance_count=0, instance_type="r5.4xlarge", applications=None, jupyter_extensions=None,
                 eggs=None, steps=None, auto_terminate=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ec2_attributes = ec2_attributes.ec2_attributes
        self.emr_config = emr_config.emr_config
        self.instance_groups = instance_groups.instance_groups
        default_applications = ['Hadoop ', 'Hive', 'Ganglia', 'JupyterHub', 'Spark', 'TensorFlow']
        # possible: ['Hadoop ', 'Hive', 'Pig', 'Hue', 'Ganglia', 'JupyterHub', 'Zeppelin', 'Spark', 'TensorFlow']
        default_jupyter_extensions = ['highlighter/highlighter', 'execute_time/ExecuteTime', 'scratchpad/main',
                                      'varInspector/main', 'hinterland/hinterland', 'snippets_menu/main',
                                      'table_beautifier/main', 'move_selected_cells/main', 'python-markdown/main',
                                      'runtools/main', 'spellchecker/main', 'toggle_all_line_numbers/main']
        self.applications = default_applications
        self.auto_terminate = auto_terminate
        if applications:
            self.applications = applications
        if auto_terminate:
            self.applications.remove('JupyterHub')
        self.jupyter_extensions = default_jupyter_extensions
        if jupyter_extensions:
            self.jupyter_extensions = jupyter_extensions
        self.bootstrap_actions = list()
        script = 'emr_setup.sh'
        s3_script = f'{self.get_script_path()}/{script}'
        self.eggs = [f'{self.get_packages_path()}/{get_egg_name()}']
        if eggs:
            this_eggs = self.eggs
            this_eggs.extend(eggs)
            self.eggs = this_eggs
        args = list()
        if self.eggs:
            args = [f'--eggs={",".join(self.eggs)}']
        if 'JupyterHub' in self.applications and self.jupyter_extensions:
            args.append(f'--extensions={",".join(self.jupyter_extensions)}')
        default_steps = list()
        default_steps.append({"Name": f'Retrieve {s3_script}', "Jar": "command-runner.jar",
                              "Args": ["aws", "s3", "cp", s3_script, "/home/hadoop/"]})
        default_steps.append({"Name": f'Add execute permissions to {script}', "Jar": "command-runner.jar",
                              "Args": ["chmod", "755", f'/home/hadoop/{script}']})
        default_steps.append({"Name": f'Run {script}', "Jar": "command-runner.jar",
                              "Args": [f'/home/hadoop/{script}', *args]})
        default_steps.append({"Name": f'Install required Python libraries', "Jar": "command-runner.jar",
                              "Args": [f'sudo', 'pip-3.6', 'install', 'boto3']})
        if self.bad_record_path:
            default_steps.append({"Name": f'Create {self.bad_record_path}', "Jar": "command-runner.jar",
                                  "Args": [f'mkdir', '-p', self.bad_record_path]})
        if steps:
            default_steps.extend(steps)
        self.steps = default_steps
        self.tweak(core_instance_count=core_instance_count, instance_type=instance_type)

    def tweak(self, core_instance_count=None, instance_type=None):
        if core_instance_count is not None:
            delitem = None
            for i, ig in enumerate(self.instance_groups):
                if ig["InstanceGroupType"] == "CORE":
                    if core_instance_count == 0:
                        delitem = i
                    ig["InstanceCount"] = core_instance_count
                    self.instance_groups[i] = ig
            if delitem is not None:
                self.instance_groups.__delitem__(delitem)
        if instance_type:
            if instance_type.find('.xlarge') > -1:
                cpus = 1
            else:
                cpus = int(re.sub(".*\\.([0-9]+)x.*", "\\1", instance_type))
            if cpus < 4:
                Warning('Too few CPUs to optimize allocation. Setting maximizeResourceAllocation to true.')
                for i, ec in enumerate(self.emr_config):
                    if ec["Classification"] == "spark":
                        ec["Properties"]["maximizeResourceAllocation"] = "true"
                    if ec["Classification"] == "yarn-site":
                        ec["Properties"] = {}
                    if ec["Classification"] == "spark-defaults":
                        ec["Properties"] = {}
                    self.emr_config[i] = ec
            for i, ig in enumerate(self.instance_groups):
                ig["InstanceType"] = instance_type
                self.instance_groups[i] = ig
            if cpus >= 4:
                for i, ec in enumerate(self.emr_config):
                    if ec["Classification"] == "spark-defaults":
                        ec["Properties"]["spark.executor.memory"] = f'{int(self.get("total_executor_memory") * 0.9)}G'
                        ec["Properties"]["spark.executor.memoryOverhead"] = \
                            f'{self.get("total_executor_memory") - int(self.get("total_executor_memory") * 0.9)}G'
                        ec["Properties"]["spark.driver.memory"] = ec["Properties"]["spark.executor.memory"]
                        ec["Properties"]["spark.driver.memoryOverhead"] = ec["Properties"]["spark.executor.memoryOverhead"]
                        ec["Properties"]["spark.executor.cores"] = str(5)
                        ec["Properties"]["spark.driver.cores"] = ec["Properties"]["spark.executor.cores"]
                        self.emr_config[i] = ec
                for i, ec in enumerate(self.emr_config):  # We require "executors_per_instance", calculated from self.
                    if ec["Classification"] == "spark-defaults":
                        ec["Properties"]["spark.executor.instances"] = \
                            str(max(self.get("executors_per_instance") * (self.get("core_instance_count") + 1) - 1, 1))
                        ec["Properties"]["spark.default.parallelism"] = \
                            str(int(ec["Properties"]["spark.executor.instances"]) * int(self.get("spark.executor.cores"))
                                * 2)
                        self.emr_config[i] = ec
                    if ec["Classification"] == "yarn-site":
                        ec["Properties"]["yarn.nodemanager.resource.memory-mb"] = \
                            str((self.get("memory_per_instance") - 1) * 1024)
                        ec["Properties"]["yarn.nodemanager.resource.cpu-vcores"] = str(self.get("vcpus") - 1)
                        self.emr_config[i] = ec
        if 'JupyterHub' in self.applications:
            for i, ec in enumerate(self.emr_config):
                if ec["Classification"] == "jupyter-s3-conf":
                    ec["Properties"]["s3.persistence.bucket"] = f'{self.get_bucket()}'
                    self.emr_config[i] = ec
        # action = {"Path": s3_script, "Name": s3_script, "Args": args}
        # self.bootstrap_actions.append(action) if action not in self.bootstrap_actions else self.bootstrap_actions

    def get(self, param):
        if param == "core_instance_count":
            out = None
            for i, ig in enumerate(self.instance_groups):
                if ig["InstanceGroupType"] == "CORE":
                    out = ig["InstanceCount"]
            if out is None:
                out = 0
            return out
        if param == "instance_type":
            out = None
            for i, ig in enumerate(self.instance_groups):
                if ig["InstanceGroupType"] == "CORE":
                    out = ig["InstanceType"]
            if out is None:
                for i, ig in enumerate(self.instance_groups):
                    if ig["InstanceGroupType"] == "MASTER":
                        out = ig["InstanceType"]
            return out
        if param == "tags":
            return f"Name=coe-ds-spark-{self.environment} User={getpass.getuser()}"
        if param == "name":
            core_instance_count = self.get("core_instance_count")
            instance_type = self.get("instance_type")
            return f'coe-ds-spark-{self.environment}-{instance_type}-{core_instance_count}c-1d'
        if param == "applications":
            applications = ''
            if self.applications:
                applications = [f'Name={app}' for app in self.applications]
                applications = ' '.join(applications)
                applications = f'--application {applications}'
            return applications
        if param.lower() == 'cpus':
            instance_type = self.get('instance_type')
            cpus = re.sub(".*\\.([0-9]+)x.*", "\\1", instance_type)
            cpus = int(cpus)
            return cpus
        if param.lower() == 'vcpus':
            return self.get("cpus") * 4
        if param in ["spark.executor.cores", "spark.executor.memory", "spark.executor.instances"]:
            for i, ec in enumerate(self.emr_config):
                if ec["Classification"] == "spark-defaults":
                    return ec["Properties"][param]
        if param.lower() == 'executors_per_instance':
            return int((self.get('vcpus') - 1) / int(self.get("spark.executor.cores")))
        if param.lower() == 'instance_category':
            instance_type = self.get('instance_type')
            return re.sub("^(.*)\\.[0-9]+x.*$", "\\1", instance_type)
        if param.lower() == 'memory_per_instance':
            instance_category = self.get('instance_category')
            if instance_category in ['r5']:
                base_memory = 8
            else:
                raise Exception(f'Instance category {instance_category} is not supported.')
            return base_memory * self.get("vcpus")
        if param.lower() == 'total_executor_memory':
            return int((self.get('memory_per_instance') - 1) / self.get("executors_per_instance"))

    def emr_create_cluster(self, run=False, deploy=True):
        cmd = f'aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole {self.get("applications")} ' \
              f'--tags \'{self.get("tags")}\' --ebs-root-volume-size 10 ' \
              f'--ec2-attributes \'{json.dumps(self.ec2_attributes)}\' --service-role EMR_DefaultRole ' \
              f'--enable-debugging --release-label emr-5.28.0 ' \
              f'--log-uri \'{self.url_prefix}/{self.get_bucket()}/aws-logs/elasticmapred/\' --name \'{self.get("name")}\' ' \
              f'--instance-groups \'{json.dumps(self.instance_groups)}\' ' \
              f'--configurations \'{json.dumps(self.emr_config)}\' ' \
              f'--scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1'
        if self.bootstrap_actions:
            cmd = f'{cmd} --bootstrap-actions \'{json.dumps(self.bootstrap_actions)}\''
        if self.steps:
            cmd = f'{cmd} --steps \'{json.dumps(self.steps)}\''
        if self.auto_terminate:
            cmd = f'{cmd} --auto-terminate'
        if not run:
            out = self.deploy_to_s3(False)
            out.append(cmd)
            return out
        else:
            print(f'Launch run in {self.environment} environment.')
            self.deploy_to_s3(deploy)
            os.system(cmd)


def parse_aws_args():
    """
    Argument parser for pipelines
    """
    parser = argparse.ArgumentParser(description='Parse AWS Args')
    parser.add_argument('--core-instance-count', metavar='int', default=0, type=int, help='Core instance count')
    parser.add_argument('--instance-type', metavar='string', default="r5.4xlarge", type=str, help='Instance type')
    parser.add_argument('--auto-terminate', metavar='python boolean', default='False', type=str,
                        help='Auto-terminate after steps')
    parser.add_argument('--environment', metavar='name', default='dev', type=str, help='Environment')
    parser.add_argument('--class-name', metavar='class', type=str, help='Pipeline class name')
    parser.add_argument('--run', metavar='python boolean', default='False', type=str, help='Whether to create cluster')

    args = parser.parse_args()
    args.auto_terminate = args.auto_terminate == 'True'
    args.run = args.run == 'True'
    args.steps = list()
    if args.class_name:
        pipeline = Pipeline.parse_pipeline_args()
        args.steps.extend(get_pipeline_step(*pipeline))
    return args


def get_pipeline_step(script, class_name, method_name, dt_range, environment, save):
    if script is None:
        return None
    args = [script, '--class-name', class_name, '--method-name', method_name, '--dt-range', json.dumps(dt_range),
            '--environment', environment, '--save', str(save)]
    step = [{"Name": f"{class_name} {method_name} Spark job", "Jar": "command-runner.jar", "Args": ['spark-submit',
                                                                                                    *args]}]
    return step


def parse_launch_pipeline_on_emr():
    """
    Argument parser for pipelines
    """
    parser = argparse.ArgumentParser(description='Parse pipeline launch Args')
    parser.add_argument('--class-name', metavar='class', type=str, help='Pipeline class name')
    parser.add_argument('--process-group', metavar='string', type=str, help='Process group name')
    parser.add_argument('--dt-start', metavar='date', type=str, help='Start date')
    parser.add_argument('--until', metavar='date', type=str, help='End date')
    parser.add_argument('--n-rounds', metavar='int', type=int, default=1, help='Number of rounds per EMR instance')
    parser.add_argument('--environment', metavar='name', default='dev', type=str, help='Environment')
    parser.add_argument('--run', metavar='python boolean', default='False', type=str, help='Whether to create cluster')
    args = parser.parse_args()
    args.run = args.run == 'True'
    return args


def launch_pipeline_on_emr(class_name, process_group, dt_start, until, n_rounds=1, environment='dev', run=False):
    submodule = class_name.lower()
    module = f'pipelines.{submodule}.{submodule}'
    module = importlib.import_module(module)
    pipeline_instance = getattr(module, class_name)(environment=environment)
    args = pipeline_instance.process_groups[process_group]
    freq_module = importlib.import_module('dateutil.rrule')
    freq = getattr(freq_module, args.freq)
    ranges = date_range(dt_start=dt_start, until=until, in_format=args.out_format, out_format=args.out_format,
                        by=args.by, freq=freq)
    # Get a list of grouped steps
    i = 0
    n_ranges = len(ranges)
    steps = list()
    steps_list = list()
    while i < n_ranges:
        dt_range = ranges[i]
        # Accumulate rounds
        for method_name in args.method_names:
            step = get_pipeline_step(PIPELINE_SCRIPT, class_name=class_name, method_name=method_name,
                                     dt_range=dt_range, environment=pipeline_instance.environment,
                                     save=pipeline_instance.save_to_object_map_path)
            steps.extend(step)
        i = i + 1
        steps_list.append(steps)
        steps = list()
    # Define batches to prioritize earliest steps
    n_steps_list = len(steps_list)
    n_clusters = ceil(n_steps_list / n_rounds)
    batch_dict = dict()
    for i in range(n_clusters):
        batch_dict[i] = list()
    j = 0
    while j < n_steps_list:
        i = 0
        while i < n_clusters and len(steps_list) > 0:
            batch_dict[i].extend(steps_list.pop(0))
            i = i + 1
        j = j + 1

    # Run batches
    if len(batch_dict) > 250:
        raise NotImplementedError('Reaching AWS max limit for 256 pending steps. Hint: Reduce n_rounds.')
    for steps in batch_dict.values():
        aws = AWS(core_instance_count=args.core_instance_count, instance_type=args.instance_type,
                  auto_terminate=args.auto_terminate, steps=steps, environment=pipeline_instance.environment)
        aws.emr_create_cluster(run=run)


if __name__ == '__main__':
    my_args = parse_aws_args()
    aws = AWS(core_instance_count=my_args.core_instance_count, instance_type=my_args.instance_type,
              auto_terminate=my_args.auto_terminate, environment=my_args.environment, steps=my_args.steps)
    aws.emr_create_cluster(run=my_args.run)
