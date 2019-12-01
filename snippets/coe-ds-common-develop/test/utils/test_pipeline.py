"""
Tests for dataframes with nested structures
"""

import unittest
import shutil
from pyspark.sql import functions as f
from utils.pipeline import Pipeline
from dataframe.utils import add_input_file_attributes


class TestPipeline(unittest.TestCase):

    def test_explode_nested_array(self):
        out = Pipeline()
        df = out.spark.read.json('../samples/nested_array.json')
        out.explode_structtype(df, recursive=False)
        expected = "[Row(ete='6894', lat=44.48, waypoints_pos=0, waypoints=Row(lat=44.88, lon=-93.22)), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=1, waypoints=Row(lat=44.85, lon=-93.11)), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=2, waypoints=Row(lat=44.82, lon=-93.04))]"
        observed = str(out.dataframes.waypoints.collect())
        self.assertEqual(expected, observed)

    def test_normalize_nested_array(self):
        out = Pipeline()
        df = out.spark.read.json('../samples/nested_array.json')
        out.explode_structtype(df, recursive=True)
        expected = "[Row(ete='6894', lat=44.48, waypoints_pos=0, waypoints_lat=44.88, waypoints_lon=-93.22), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=1, waypoints_lat=44.85, waypoints_lon=-93.11), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=2, waypoints_lat=44.82, waypoints_lon=-93.04)]"
        observed = str(out.dataframes.waypoints_waypoints.collect())
        self.assertEqual(expected, observed)
        expected = "[Row(ete='6894', lat=44.48, waypoints_pos=0, waypoints=Row(lat=44.88, lon=-93.22)), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=1, waypoints=Row(lat=44.85, lon=-93.11)), "\
                   "Row(ete='6894', lat=44.48, waypoints_pos=2, waypoints=Row(lat=44.82, lon=-93.04))]"
        observed = str(out.dataframes.waypoints.collect())
        self.assertEqual(expected, observed)

    def test_split_structure(self):
        tmp = Pipeline()
        df = tmp.spark.read.json('../samples/nested_array.json')
        tmp.explode_structtype(df, recursive=False)
        df = tmp.dataframes.waypoints
        out = Pipeline()
        out.structure_split(df, structure_columns=['waypoints_pos'], explode_structtype=False)
        observed0 = str(out.dataframes['0'].collect())
        observed1 = str(out.dataframes['1'].collect())
        observed2 = str(out.dataframes['2'].collect())
        expected0 = "[Row(ete='6894', lat=44.48, waypoints_pos=0, waypoints=Row(lat=44.88, lon=-93.22))]"
        expected1 = "[Row(ete='6894', lat=44.48, waypoints_pos=1, waypoints=Row(lat=44.85, lon=-93.11))]"
        expected2 = "[Row(ete='6894', lat=44.48, waypoints_pos=2, waypoints=Row(lat=44.82, lon=-93.04))]"
        self.assertEqual(expected0, observed0)
        self.assertEqual(expected1, observed1)
        self.assertEqual(expected2, observed2)

    def test_split_structure_and_explode_structtype(self):
        tmp = Pipeline()
        df = tmp.spark.read.json('../samples/nested_array.json')
        tmp.explode_structtype(df, recursive=False)
        df = tmp.dataframes.waypoints
        out = Pipeline()
        out.structure_split(df, structure_columns=['waypoints_pos'], explode_structtype=True)
        observed0 = str(out.dataframes['0_waypoints'].collect())
        observed1 = str(out.dataframes['1_waypoints'].collect())
        observed2 = str(out.dataframes['2_waypoints'].collect())
        expected0 = "[Row(ete='6894', lat=44.48, waypoints_pos=0, waypoints_lat=44.88, waypoints_lon=-93.22)]"
        expected1 = "[Row(ete='6894', lat=44.48, waypoints_pos=1, waypoints_lat=44.85, waypoints_lon=-93.11)]"
        expected2 = "[Row(ete='6894', lat=44.48, waypoints_pos=2, waypoints_lat=44.82, waypoints_lon=-93.04)]"
        self.assertEqual(expected0, observed0)
        self.assertEqual(expected1, observed1)
        self.assertEqual(expected2, observed2)

    def test_catalog_runs(self):
        out = Pipeline()
        df = out.spark.read.json('../samples/nested_array.json')
        out.explode_structtype(df, recursive=True)
        out.catalog()

    def test_save_and_load(self):
        out = Pipeline()
        df = out.spark.read.json('../samples/nested_array.json')
        out.explode_structtype(df, recursive=True)
        path = './test_save_and_load'
        out.nested_run(path=path)
        # Test retrieving tables
        counts = []
        for name in list(out.dataframes.keys()):
            counts.append(out.spark.read.load(f'{path}/{name}').count())
            out.spark.sql(f'DROP TABLE IF EXISTS {name}')
            out.spark.sql(f"CREATE TABLE {name} USING PARQUET LOCATION '{path}/{name}'")
            out.spark.sql(f'DROP TABLE {name}')
            # out.spark.sql(f"OPTIMIZE {name} ZORDER BY (DayofWeek)")
        shutil.rmtree(path)
        self.assertEqual(counts, [3, 3])

    def test_read(self):
        out = Pipeline()
        print(out.reader())

    def test_pipeline_add_partitions(self):
        out = Pipeline()
        df = out.spark.read.json('../samples/FlightAwareFirehose_2019-09-10_0004.txt')
        df = add_input_file_attributes(df, rid_regex='\\d{1,4}-\\d{1,2}-\\d{1,2}_\\d{1,4}',
                                       rid_format='yyyy-MM-dd_HHmm')

        out.structure_split(df, structure_columns=['type'])
        for timestamp_column in ['edt', 'eta', 'fdt', 'pitr']:
            out.apply_transformations(timestamp_column, [timestamp_column],
                                      f.col(timestamp_column).cast('long').cast('timestamp'))

        out.add_dataframes_options(partition_by=["record_ingest_date"], repartition=1,
                                   sort_within_partitions=['id'])
        fp_names = [key for key in list(out.dataframes.keys()) if key.find('flightplan') > -1]
        out.add_dataframes_options(partition_by=["record_ingest_hour"], dataframe_names=fp_names)
        wp_names = [key for key in list(out.dataframes.keys()) if
                    key.find('flightplan') > -1 and key.find('waypoints') > -1]
        out.add_dataframes_options(repartition=10, dataframe_names=wp_names)
        path = './test_pipeline_add_partitions'
        with self.assertRaises(Exception) as context:
            out.run(path)
        self.assertTrue('Expecting a DataFrame' in str(context.exception))
        out.nested_run(path)
        shutil.rmtree(path)

    def test_retrieve_dependencies(self):
        out = Pipeline()
        out.dependencies.__setattr__("der_id_reg", ['flightaware'])
        out.dependencies.__setattr__("der_latest_fadooce", ['der_id_reg'])
        out.dependencies.__setattr__("der_flightplan_next", ['der_latest_fadooce'])
        out.dependencies.__setattr__("der_combined_latest", ['der_flightplan_next'])
        out.dependencies.__setattr__("der_turnaround", ['der_combined_latest'])
        self.assertEqual(out._retrieve_dependencies('der_flightplan_next'),
                         ['flightaware', 'der_id_reg', 'der_latest_fadooce'])
        self.assertEqual(out._retrieve_dependencies('der_turnaround'),
                         ['flightaware', 'der_id_reg', 'der_latest_fadooce', 'der_flightplan_next',
                          'der_combined_latest'])

    def test_retrieve_empty_dependencies(self):
        out = Pipeline()
        self.assertEqual(out._retrieve_dependencies('der_flightplan_next'), None)

if __name__ == '__main__':
    unittest.main()
