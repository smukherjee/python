"""
Utilities to spark the Spark
"""

import configparser
import os, sys
from pyspark.sql import SparkSession


class SparkSingleton:
    """
    Class to spark the Spark
    """

    @staticmethod
    def get(app_name=None):
        """
        Get the Spark singleton.
        :param app_name: Application name
        :type app_name: string
        :return: A SparkSession object
        :rtype: SparkSession
        """
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()

        # spark.databricks.delta.optimizeWrite.enabled
        # spark.databricks.delta.autoCompact.enabled

        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # spark._jsc.hadoopConfiguration().set("spark.jars.packages", "io.delta:delta-core_2.11:0.4.0")
        spark._jsc.hadoopConfiguration().set("fs.s3a.buffer.dir", "/var/tmp/spark")
        if False:
            # This is not working
            config = configparser.ConfigParser()
            config.read(os.path.expanduser("~/.aws/credentials"))
            aws_profile = 'saml'
            access_id = config.get(aws_profile, "aws_access_key_id")
            access_key = config.get(aws_profile, "aws_secret_access_key")
            access_token = config.get(aws_profile, "aws_session_token")
            spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", access_id)
            spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", access_key)
            spark._jsc.hadoopConfiguration().set("fs.s3a.session.token", access_token)

        sc = spark.sparkContext
        print(f'Spark Session: {spark.__dict__["_sc"].__dict__["appName"]}')
        print(sys.version, {"defaultParallelism": sc.defaultParallelism,
                            "ExecutorMemoryStatus.size": sc._jsc.sc().getExecutorMemoryStatus().keySet().size()})

        return spark

