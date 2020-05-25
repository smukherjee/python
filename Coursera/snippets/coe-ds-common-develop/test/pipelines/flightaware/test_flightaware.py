"""
Tests for pipeline.
"""

import unittest
import shutil
from pipelines.flightaware.flightaware import FlightAware


class TestFlightAware(unittest.TestCase):

    def test_flightaware_run_pipeline(self):
        out = FlightAware()
        out.url_prefix = '.'
        out.bucket_prefix = 'test'
        #out.arguments.file_list = 'samples/FlightAwareFirehose_2019-09-10_0004_small.txt'
        #out.arguments.file_list = 'samples/FlightAwareFirehose_2019-09-10_0004.txt'
        out.arguments.file_list = 'samples/FlightAwareFirehose_2019-09-10_0004_small.txt'
        out.run_all_pipelines()
        shutil.rmtree(f'{out.url_prefix}/{out.get_bucket()}')


