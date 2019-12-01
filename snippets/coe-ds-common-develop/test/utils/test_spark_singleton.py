"""
Test spark singleton classes
"""

import unittest
from utils.spark_singleton import SparkSingleton


class TestSparkSingleton(unittest.TestCase):

    def test_get_returns_spark_session(self):
        spark = SparkSingleton.get()
        self.assertEqual(spark.__class__.__name__, 'SparkSession')
        spark.stop()

    def test_get_with_app_name(self):
        spark = SparkSingleton.get('test_app')
        self.assertEqual(spark.__class__.__name__, 'SparkSession')
        spark.stop()


if __name__ == '__main__':
    unittest.main()
