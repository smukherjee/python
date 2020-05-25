"""
Tests for flightaware source
"""

import unittest
from registry.sources.flightaware import FlightAware


class TestFlightAware(unittest.TestCase):

    def test_flightaware(self):
        out = FlightAware()
        print(out)
        print(out.metrics_family.Aircraft.retrieve())


if __name__ == '__main__':
    unittest.main()
