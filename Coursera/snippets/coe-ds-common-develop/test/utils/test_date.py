"""
Tests for date utilities
"""


import unittest
from datetime import date, datetime
from utils.date import date_range


class TestDate(unittest.TestCase):

    def test_date_range_no_format(self):
        a = date(2009, 5, 8)
        b = date(2009, 5, 9)
        self.assertEqual(date_range(a, b),
                         [datetime(2009, 5, 8), datetime(2009, 5, 9)])

    def test_date_range_reverse(self):
        a = date(2009, 5, 15)
        b = date(2009, 5, 14)
        self.assertEqual(date_range(a, b),
                         [datetime(2009, 5, 15), datetime(2009, 5, 14)])

    def test_date_range_with_formats(self):
        self.assertEqual(date_range('6/2/2013', '6/3/2013', in_format="%m/%d/%Y"),
                         [datetime(2013, 6, 2), datetime(2013, 6, 3)])
        self.assertEqual(date_range('6/2/2013', '6/3/2013', in_format="%m/%d/%Y", out_format="%Y-%m-%d"),
                         ['2013-06-02', '2013-06-03'])

    def test_date_range_with_monthly_output(self):
        self.assertEqual(date_range('8/2/2013', '6/3/2013', in_format="%m/%d/%Y", out_format="%Y-%m"),
                         ['2013-08', '2013-07', '2013-06'])

    def test_date_range_with_by(self):
        expected = [['2013-06-13', '2013-06-12', '2013-06-11', '2013-06-10', '2013-06-09'],
                    ['2013-06-08', '2013-06-07', '2013-06-06', '2013-06-05', '2013-06-04'], ['2013-06-03']]
        self.assertEqual(date_range('6/13/2013', '6/3/2013', in_format="%m/%d/%Y", out_format="%Y-%m-%d", by=5),
                         expected)
