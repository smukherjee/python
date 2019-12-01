"""
Pipeline Template
"""

import argparse
import importlib
import inspect
import json
import time
from copy import copy
from os.path import basename
from boto3 import client
from collections import OrderedDict
from pyspark.sql import functions as f
from configs.common import Common, PIPELINE_SCRIPT
from dataframe.utils import save, get_fields_of_type, get_primary_records
from utils.date import date_range
from utils.map import Map
from utils.spark_singleton import SparkSingleton


class Pipeline(Common):
    """
    Pipeline Template
    """

    def __init__(self, app_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_name = app_name
        self.spark = SparkSingleton().get(self.app_name)
        self.dataframes = Map()
        self.dataframes_options = Map()
        self.dataframes_omit = []
        self.columns_by_dataframe = Map()
        self.object_map = Map()
        self.pipeline_order = []
        self.process_groups = Map()
        self.target_structures = []
        self.abbreviations = Map()
        # self.time_profiles = Map()
        self.dependencies = Map()
        self.satisfied = Map()
        self.arguments = Map()
        self.save_to_object_map_path = True

    def reader(self, parent_function=None):
        reader = self.spark.read
        if self.bad_record_path:
            reader = reader.option("badRecordsPath", self.bad_record_path)
        if not parent_function:
            parent_function = inspect.stack()[1].function
        schema_name = f'_{parent_function}_schema'.upper()
        try:
            schema = self[schema_name]
            reader = reader.schema(schema)
        except KeyError as e:
            if hasattr(e, 'message'):
                print(e.message)
            print(Exception(f'You can save processing time by defining the {schema_name} object.'))
        return reader

    def propose_schema(self, df, parent_function=None):
        if not parent_function:
            parent_function = inspect.stack()[1].function
        schema_name = f'_{parent_function}_schema'.upper()
        try:
            schema = self[schema_name]
        except KeyError as e:
            if hasattr(e, 'message'):
                print(e.message)
            print(Exception(f'Here is a proposed {schema_name} object.:\n{df.schema}'))

    def run(self, df, name=None, path=None, parent_function=None, **options):
        """
        Save dataframes from NestedStructures instance

        :param df:
        :type df:
        :param name:
        :type name:
        :param path: the path in a Hadoop supported file system
        :type path:
        :param partitionBy: names of partitioning columns
        :param options: all other string options
        :return:
        :rtype:
        """
        if df.__class__.__name__ != 'DataFrame':
            raise TypeError('Expecting a DataFrame as first argument. Did you mean to call nested_save instead?')
        if not name and not path:
            raise Exception('Must supply at least one of "name" or "path" arguments.')
        if not name:
            name = basename(path)
        if not path:
            path = f'{self.get_write_path()}/{name}'
        self.dataframes.__setattr__(name, df)
        self.object_map.__setattr__(name, path)
        self.catalog(name)

        if self.save_to_object_map_path:
            partition_by = self.dataframes_options.__getattr__(name).partition_by
            repartition = self.dataframes_options.__getattr__(name).repartition
            sort_within_partitions = self.dataframes_options.__getattr__(name).sort_within_partitions
            write_format = self.dataframes_options.__getattr__(name).format
            mode = self.dataframes_options.__getattr__(name).mode
            save(df, path=self.object_map.__getattr__(name), partition_by=partition_by, repartition=repartition,
                 sort_within_partitions=sort_within_partitions, format=write_format, mode=mode, **options)

        # Mark satisfied dependency
        if not parent_function:
            parent_function = inspect.stack()[1].function
        if parent_function not in self.satisfied.keys():
            self.satisfied[parent_function] = list()
        self.satisfied[parent_function].append(name)

    def load(self, name, logical=False, parent_function=None):
        if not parent_function:
            parent_function = inspect.stack()[1].function
        if name not in self.dataframes.keys():
            self.run_dependencies(parent_function)
        if logical:
            print(f'Loading logical dataframe "{name}".')
            return self.dataframes[name]
        path = self.object_map[name]
        try:
            print(f'Try loading "{name}" from "{path}"...')
            start = time.time()
            df = self.spark.read.load(path)
            # We need a separate line of code, otherwise self.dataframes[name] can be initiated on error.
            self.dataframes[name] = df
            end = time.time()
            print(f'Success! {round(end - start, 1)} seconds')
            return self.dataframes[name]
        except:
            print(f'"{path}" does not exist.')
            self.load(name, logical=True, parent_function=parent_function)


    def explode_structtype(self, df, root_name='', recursive=True):
        """
        Explode structtype columns into dataframes.
        :param df: A spark dataframe
        :type df: DataFrame
        :param root_name: The root_name of the Map
        :type root_name: String
        :param recursive: Whether to recursively explode nested StructType columns
        :type recursive: Boolean
        :return: A map of dataframes
        :rtype: Map
        """
        structtype_fields = get_fields_of_type(df, data_type='StructType')
        if len(structtype_fields) > 0:
            for field in structtype_fields:
                mainly_structtype = str(field.dataType).find('StructType') == 0
                if mainly_structtype:
                    for subfield in field.dataType.fields:
                        df = df.withColumn("{}_{}".format(field.name, subfield.name), df[field.name][subfield.name])
                    df = df.drop(field.name)
                else:
                    columns_without_field = [col for col in df.columns if col != field.name]
                    df = df.select(*columns_without_field, f.posexplode(df[field.name])). \
                        withColumnRenamed('col', field.name). \
                        withColumnRenamed('pos', '{}_pos'.format(field.name))
                map_name = field.name
                if len(root_name):
                    map_name = "{}_{}".format(root_name, field.name)
                try:
                    keep_columns = self.columns_by_dataframe[map_name]
                except KeyError:
                    keep_columns = None
                if keep_columns:
                    df = df.select(*keep_columns)
                else:
                    print(Warning(f"You could save processing time by explicitly listing out columns to keep under "
                                  f"Pipeline().columns_by_dataframe.{map_name} object."))
                self.dataframes.__setattr__(map_name, df)
                self.dataframes_options.__setattr__(map_name, Map())

                if recursive:
                    self.explode_structtype(self.dataframes.__getattr__(map_name), root_name=map_name, recursive=True)

    def structure_split(self, df, structure_columns=None, target_structures=None, explode_structtype=True,
                        recursive=True):
        """
        Split a Spark dataframe into multiple dataframes to isolate the different (nested) structures.
        :param df: A Spark dataframe
        :type df: DataFrame
        :param structure_columns: column names by which to split the original structure
        :type structure_columns: list of strings
        :param target_structures:
        :type target_structures:
        :param explode_structtype: Whether to explode columns with a nested StructType
        :type explode_structtype: Boolean
        :param recursive: Whether to recursively explode nested StructType columns
        :type recursive: Boolean
        :return: A map of dataframes
        :rtype: Map
        """
        if structure_columns:
            # df = df.repartition(200).sortWithinPartitions(*structure_columns)
            df.cache()
            if not target_structures:
                target_structures = [rec_type[0] for rec_type in df.select(*structure_columns).distinct().collect()]
                print(Exception(f'You can save processing time by defining the "target_structures" argument.\n'
                                f'A suggested value is:\n'
                                f'target_structures = {target_structures}'))
            for rec_type in target_structures:
                df_type = df.filter(df[structure_columns[0]] == f.lit(rec_type))  # This supports only one column
                try:
                    keep_columns = self.columns_by_dataframe[str(rec_type)]
                except KeyError:
                    keep_columns = None
                if not keep_columns:
                    # Remove columns where all values are NULL
                    keep_columns = [c for c in df_type.columns
                                    if df_type.select(c).filter(f.col(c).isNotNull()).first() is not None]
                    print(Warning(f"You could save processing time by explicitly listing out columns to keep under "
                                  f"Pipeline().columns_by_dataframe.{str(rec_type)} object.\nThe detected list "
                                  f"is:\n"
                                  f"{keep_columns}"))
                df_type = df_type.select(*keep_columns)
                self.dataframes.__setattr__(str(rec_type), df_type)
                self.dataframes_options.__setattr__(str(rec_type), Map())
            df.unpersist()
        if explode_structtype:
            df_keys = list(self.dataframes.keys())
            for key_name in df_keys:
                df_item = self.dataframes.__getattr__(key_name)
                self.explode_structtype(df_item, root_name=key_name, recursive=recursive)

    def catalog(self, dataframe_names='<all>'):
        """
        Generate a catalog of dataframe
        :return:
        :rtype:
        """
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        cat = {}
        for name in dataframe_names:
            df = self.dataframes.__getattr__(name)
            print(f'Dataframe name: {name}')
            df.printSchema()

    def add_dataframes_options(self, partition_by=None, repartition=None, sort_within_partitions=None, format=None,
                               mode=None, dataframe_names='<all>', overwrite=False):
        """

        :param partition_by:
        :type partition_by:
        :param dataframe_names:
        :type dataframe_names:
        :return:
        :rtype:
        """
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        opt_names = []
        for opt in ['partition_by', 'repartition', 'sort_within_partitions', 'format', 'mode']:
            this_opt = locals()[opt]
            if this_opt:
                for name in dataframe_names:
                    opt_value = []
                    cur_options = self.dataframes_options.__getattr__(name)
                    if overwrite:
                        cur_options = None
                    if cur_options and opt in list(cur_options.keys()):
                        opt_value = cur_options[opt]
                    if opt in ['repartition', 'format', 'mode'] or len(opt_value) < 1:
                        opt_value = this_opt
                    else:
                        opt_value = copy(opt_value)  # Workaround for value propagating to all dataframe names
                        opt_value.extend(this_opt)
                    all_opt = self.dataframes_options.__getattr__(name)
                    if not all_opt:
                        all_opt = Map()
                    all_opt.__setattr__(opt, opt_value)
                    self.dataframes_options.__setattr__(name, all_opt)

    def apply_transformations(self, target_column, required_columns, expression, dataframe_names='<all>'):
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        for name in dataframe_names:
            df = self.dataframes.__getattr__(name)
            columns = df.columns
            if all(elem in columns for elem in required_columns):
                df = df.withColumn(target_column, expression)
                self.dataframes.__setattr__(name, df)

    def get_latest_records(self, partition_by, order_by, join_df=None, on=None, how='inner', load_filter=None, filter=None,
                           select=None, drop=None, dataframe_names='<all>', prefix='der_', suffix='_latest',
                           parent_function=None):
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        if not parent_function:
            parent_function = inspect.stack()[1].function
        out_names = []
        for name in dataframe_names:
            df = self.load(name, parent_function=parent_function)
            if load_filter is not None:
                df = df.filter(load_filter)
            df = get_primary_records(df, partition_by=partition_by, order_by=order_by, join_df=join_df, on=on, how=how,
                                     filter=filter, select=select, drop=drop, rank=1)
            new_name = f'{prefix}{name}{suffix}'
            out_names.append(new_name)
            self.dataframes.__setattr__(new_name, df)
            self.dataframes_options.__setattr__(new_name, self.dataframes_options.__getattr__(name))
            self.abbreviations.__setattr__(new_name, self.abbreviations.__getattr__(name))
        return out_names

    def remap_columns_from_source(self, id_columns, remap_columns, remap_source_name, dataframe_names='<all>',
                                  prefix='der_', suffix=None, how='left', broadcast=True,
                                  parent_function=None):
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        if not suffix:
            suffix = f'_{"_".join(remap_columns)}'
        if not parent_function:
            parent_function = inspect.stack()[1].function
        src = self.load(remap_source_name, parent_function=parent_function). \
            select(*id_columns, *remap_columns)
        if broadcast:
            f.broadcast(src)
        out_names = []
        for name in dataframe_names:
            df = self.load(name, parent_function=parent_function)
            df = df.drop(*remap_columns).join(src, on=id_columns, how=how)
            new_name = f'{prefix}{name}{suffix}'
            out_names.append(new_name)
            self.dataframes.__setattr__(new_name, df)
            self.dataframes_options.__setattr__(new_name, self.dataframes_options.__getattr__(name))
            self.abbreviations.__setattr__(new_name, self.abbreviations.__getattr__(name))
        src.unpersist()
        return out_names

    def join_sources(self, id_columns, dataframe_names, combined_name, how='left', load_filter=None, parent_function=None):
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        first_name = dataframe_names.pop(0)
        if not parent_function:
            parent_function = inspect.stack()[1].function
        df = self.load(first_name, parent_function=parent_function)
        if load_filter is not None:
            df = df.filter(load_filter)
        print(f'join_sources() call, starting with {first_name}:')
        for name in dataframe_names:
            name_df = self.load(name, parent_function=parent_function)
            if load_filter is not None:
                name_df = name_df.filter(load_filter)
            print(f'- {how} joining {name}, using {id_columns}')
            name_columns = [col for col in name_df.columns if col not in id_columns]
            abbr = self.abbreviations.__getattr__(name)
            if abbr:
                for v in name_columns:
                    name_df = name_df.withColumnRenamed(v, f"{v}_{abbr}")
            else:
                delete_columns = list(set(df.columns) & set(name_df.columns))
                delete_columns = [col for col in delete_columns if col not in id_columns]
                if len(delete_columns) > 0:
                    print(f'-- ignore conflicting column(s) {delete_columns} from {name}')
                    name_df = name_df.drop(*delete_columns)
            df = df.join(name_df, on=id_columns, how=how)
        print('Finished join_sources() call.')
        self.dataframes.__setattr__(combined_name, df)
        self.dataframes_options.__setattr__(combined_name, self.dataframes_options.__getattr__(first_name))
        return combined_name

    def filter_values(self, filter=None, dataframe_names='<all>'):
        if filter is not None:
            dataframe_names = self._retrieve_dataframe_names(dataframe_names)
            for name in dataframe_names:
                df = self.dataframes.__getattr__(name)
                print(f'Apply filter on dataframe {name}: {filter}')
                df = df.filter(filter)
                self.dataframes.__setattr__(name, df)

    def nested_run(self, path=None, parent_function=None, dataframe_names='<all>', **options):
        """
        Save dataframes from NestedStructures instance

        :param path: the path in a Hadoop supported file system
        :param partitionBy: names of partitioning columns
        :param options: all other string options
        :return:
        :rtype:
        """
        dataframe_names = self._retrieve_dataframe_names(dataframe_names)
        if not path:
            path = self.get_write_path()
        if not parent_function:
            parent_function = inspect.stack()[1].function
        for name in dataframe_names:
            df = self.dataframes.__getattr__(name)
            write_path = f'{path}/{name}'
            self.run(df, name=name, path=write_path, parent_function=parent_function)

    def run_all_pipelines(self):
        for pipeline in self.pipeline_order:
            pipeline()

    def run_dependencies(self, method_name, dt=None, save_dependencies=False):
        if method_name not in self.satisfied.keys():
            dependencies = self.dependencies.__getattr__(method_name)
            if dependencies:
                print(f'{method_name} dependencies found:')
                save_to_object_map_path = self.save_to_object_map_path
                self.save_to_object_map_path = save_dependencies
                for method_dependency in dependencies:
                    if method_dependency in self.satisfied.keys():
                        print(f"-- Dependency {method_dependency} already satisfied...")
                    else:
                        print(f"-- Applying {method_dependency} dependency...")
                        getattr(self, method_dependency)(dt=dt)
                self.save_to_object_map_path = save_to_object_map_path
            else:
                print(f'No dependencies found for {method_name} method.')

    def _retrieve_dependencies(self, method_name):
        # Dead code?
        dependencies = self.dependencies.__getattr__(method_name)
        if not dependencies:
            return None
        dependencies.reverse()
        for method_dependency in copy(dependencies):
            sub_dependencies = self._retrieve_dependencies(method_dependency)
            if sub_dependencies:
                sub_dependencies.reverse()
                dependencies.extend(sub_dependencies)
        dependencies.reverse()
        return list(OrderedDict.fromkeys(dependencies))

    def get_s3_file_list(self, bucket, prefix='', post_prefix_list=None):
        """
        Get S3 file list.
        :param bucket: S3 bucket name
        :type bucket: string
        :param prefix: File prefix to match
        :type prefix: string
        :param post_prefix_list: List of post-prefix strings to match
        :type post_prefix_list: list
        :return: A list of S3 files
        :rtype: list
        """
        file_list = []
        if type(prefix) not in [list, tuple]:
            prefix = [prefix]
        if not post_prefix_list:
            post_prefix_list = ['']
        s3_conn = client('s3')
        for this_prefix in prefix:
            for this_post_prefix in [f'{this_prefix}{post_prefix}' for post_prefix in post_prefix_list]:
                s3_result = s3_conn.list_objects_v2(Bucket=bucket, Prefix=this_post_prefix, Delimiter="/")
                for key in s3_result['Contents']:
                    file_list.append(key['Key'])
                print(f"List count = {len(file_list)}")
                while s3_result['IsTruncated']:
                    continuation_key = s3_result['NextContinuationToken']
                    s3_result = s3_conn.list_objects_v2(Bucket=bucket, Prefix=this_post_prefix, Delimiter="/",
                                                        ContinuationToken=continuation_key)
                    for key in s3_result['Contents']:
                        file_list.append(key['Key'])
                    print(f"List count = {len(file_list)}")
        file_list = [f"{self.__getattr__('url_prefix')}/{s3_result['Name']}/{file}" for file in file_list]
        return file_list

    def _retrieve_dataframe_names(self, dataframe_names='<all>'):
        if dataframe_names == '<all>':
            dataframe_names = list(self.dataframes.keys())
        if type(dataframe_names) is str:
            dataframe_names = [dataframe_names]
        if self.dataframes_omit:
            dataframe_names = [name for name in dataframe_names if name not in self.dataframes_omit]
        return dataframe_names

    @staticmethod
    def parse_pipeline_args():
        """
        Argument parser for pipelines
        """
        parser = argparse.ArgumentParser(description='Parse Pipeline Args')
        parser.add_argument('--class-name', metavar='class', type=str, help='Pipeline class name')
        parser.add_argument('--method-name', metavar='method', type=str, help='Pipeline method name')
        parser.add_argument('--dt-start', metavar='date', type=str, help='Start date')
        parser.add_argument('--until', metavar='date', type=str, help='End date')
        parser.add_argument('--in-format', metavar='format', default="%Y-%m-%d", type=str, help='Start date format')
        parser.add_argument('--out-format', metavar='format', type=str, help='End date format')
        parser.add_argument('--by', metavar='int', type=int, help='Chunk size')
        parser.add_argument('--dt-range', metavar='json', type=str, help='Custom json list of date strings')
        parser.add_argument('--environment', metavar='name', default='dev', type=str, help='Environment')
        parser.add_argument('--mart', metavar='folder', default='mart', type=str, help='Mart folder name')
        parser.add_argument('--save', metavar='python boolean', default='True', type=str, help='Whether to write output')
        args = parser.parse_args()
        if not args.until:
            args.until = args.dt_start
        if not args.out_format:
            args.out_format = args.in_format
        args.save = args.save == 'True'
        if not args.method_name:
            args.method_name = args.class_name.lower()
        if not args.dt_range:
            args.dt_range = date_range(dt_start=args.dt_start, until=args.until, in_format=args.in_format,
                                       out_format=args.out_format)
        else:
            args.dt_range = json.loads(args.dt_range)
        args.script = PIPELINE_SCRIPT
        print(args)
        return args


def run_main_pipeline(my_args):
    submodule = my_args.class_name.lower()
    module = f'pipelines.{submodule}.{submodule}'
    module = importlib.import_module(module)
    out = getattr(module, my_args.class_name)()
    out.environment = my_args.environment
    out.mart_folder = my_args.mart
    out.save_to_object_map_path = my_args.save
    getattr(out, my_args.method_name)(dt=my_args.dt_range)


if __name__ == '__main__':
    my_args = Pipeline.parse_pipeline_args()
    run_main_pipeline(my_args)
