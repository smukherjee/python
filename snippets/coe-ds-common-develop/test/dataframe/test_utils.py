"""
Tests for dataframe utilities.
"""

import unittest
from dataframe.utils import *
from utils.spark_singleton import SparkSingleton

spark = SparkSingleton.get()


class TestUtils(unittest.TestCase):

    def test_get_fields_of_type_identifies_structtype(self):
        df = spark.read.json('../samples/nested_array.json')
        expected = '[StructField(waypoints,ArrayType(StructType(List(StructField(lat,DoubleType,true),'\
                   'StructField(lon,DoubleType,true))),true),true)]'
        observed = str(get_fields_of_type(df, 'StructType'))
        self.assertEqual(expected, observed)

    def test_clone(self):
        df_orig = spark.read.json('../samples/nested_array.json')
        orig = df_orig.schema
        df = clone(df_orig)
        new = df.schema
        self.assertEqual(orig, new)
        self.assertEqual(df_orig.count(), df.count())
