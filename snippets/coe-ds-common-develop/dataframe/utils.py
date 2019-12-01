"""
Spark dataframe utilities
"""
import copy
import os
import pyspark.sql.functions as f
from pyspark.sql.types import LongType, IntegerType
from pyspark.sql.window import Window


def save(df, path=None, partition_by=None, repartition=None, sort_within_partitions=None, format=None, mode=None,
         **options):
    """
    Save dataframes from NestedStructures instance

    :param path: the path in a Hadoop supported file system
    :param format: the format used to save
    :param mode: specifies the behavior of the save operation when data already exists.

        * ``append``: Append contents of this :class:`DataFrame` to existing data.
        * ``overwrite``: Overwrite existing data.
        * ``ignore``: Silently ignore this operation if data already exists.
        * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
            exists.
    :param partitionBy: names of partitioning columns
    :param options: all other string options
    :return:
    :rtype:
    """

    if not path:
        raise Exception("Required path argument is missing.")
    if not mode:
        mode ='error'
    if not format:
        format = 'parquet'

    name = os.path.basename(path)
    msg = f'Writing {name} structure to path "{path}"'
    df.explain(True)
    cmd = df
    if not repartition:
        repartition = 1
    else:
        msg = f'{msg}, repartitioning into {repartition} sub-partitions'
    if partition_by:
        cmd = cmd.repartition(repartition, *partition_by)
        msg = f'{msg}, by {partition_by}'
    else:
        cmd = cmd.repartition(repartition)
    if sort_within_partitions:
        cmd = cmd.sortWithinPartitions(sort_within_partitions)
        msg = f"{msg}, sorted by {sort_within_partitions}"
    cmd = cmd.write
    if partition_by:
        cmd = cmd.partitionBy(*partition_by)
    print(msg)
    cmd.save(path, format=format, mode=mode, **options)
    print(' Done.\n')


def add_input_file_attributes(df, rid_source="_input_file_name", rid_regex=None, rid_format="yyyy-MM-dd HH:mm:ss",
                              date_source='_record_ingest_date', time_source='_record_ingest_date'):
    """
    Get input file attributes into Dataframe
    :param df: A Spark dataframe
    :type df: DataFrame
    :return: A Spark dataframe with input file attributes
    :rtype: DataFrame
    """
    df = df.withColumn('_input_file_name', f.input_file_name())
    if rid_regex:
        df = df.withColumn('_record_ingest_date', f.to_timestamp(f.regexp_extract(rid_source, rid_regex, 0),
                                                                 rid_format))
    if date_source:
        df = df.withColumn('record_ingest_date', f.to_date(date_source))
    if time_source:
        df = df.withColumn('record_ingest_hour', f.date_format(date_source, 'HH').cast(IntegerType()))
        df = df.withColumn('record_ingest_time', f.date_format(date_source, 'HH:mm'))
    return df


def get_fields_of_type(df, data_type='StructType'):
    """
    Retrieve the Dataframe's schema fields list with columns matching a given data type
    :param df: A Spark dataframe
    :type df: DataFrame
    :param data_type: The desired data type to look for
    :type data_type: string
    :return: A schema fields list matching a given data type
    :rtype: list
    """
    return [f for f in df.schema.fields if str(f.dataType).find(data_type) > -1]


def clone(df):
    """
    Inspired from https://stackoverflow.com/questions/52287553/how-to-create-a-copy-of-a-dataframe-in-pyspark.
    :param df:
    :type df:
    :return:
    :rtype:
    """
    _schema = copy.deepcopy(df.schema)
    _schema.add('__id_col', LongType(), False)  # modified inplace
    _df = df.rdd.zipWithIndex().map(lambda l: list(l[0]) + [l[1]]).toDF(_schema)
    return _df.drop('__id_col')


def get_primary_records(df, partition_by, order_by, join_df=None, on=None, how='inner', filter=None, select=None,
                        drop=None, rank=1):
    if on is None:
        on = list(set(df.columns) & set(join_df.columns))
    window = Window.partitionBy(*partition_by).orderBy(*order_by)
    if join_df is not None:
        df = df.join(join_df, on=on, how=how)
    if filter is not None:
        df = df.filter(filter)
    # We use row_number() because they may be conflicting reg values received at exactly the same time.
    # Ex.: id='KTK473-1567485954-airline-0118'
    df = df.withColumn('__rank', f.row_number().over(window)).filter(f.col('__rank') == rank).drop('__rank')
    if select:
        df = df.select(*select)
    if drop:
        df = df.drop(*drop)
    return df
