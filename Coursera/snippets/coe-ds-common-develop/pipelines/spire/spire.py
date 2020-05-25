"""
Spire Pipeline
"""

from configs.common import PIPELINE_SCRIPT
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from dataframe.utils import add_input_file_attributes
from utils.map import Map
from utils.pipeline import Pipeline
from registry.constants import DATA_TYPE_DICT


class Spire(Pipeline):
    """
    Spire Pipeline
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline_order = [self.spire]
        self.process_groups.raw = Map(script=PIPELINE_SCRIPT, method_names=['spire'], core_instance_count=0,
                                      instance_type="r5.2xlarge", out_format='%Y-%m-%d', by=1, auto_terminate=True)
        self._SPIRE_SCHEMA = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('icao_address', StringType(), True),
            StructField('latitude', DATA_TYPE_DICT.lat, True),
            StructField('longitude', DATA_TYPE_DICT.lon, True),
            StructField('altitude_baro', IntegerType(), True),
            StructField('speed', DATA_TYPE_DICT.speed, True),
            StructField('heading', DATA_TYPE_DICT.heading, True),
            StructField('callsign', StringType(), True),
            StructField('source', StringType(), True),
            StructField('collection_type', StringType(), True),
            StructField('vertical_rate', IntegerType(), True),
            StructField('squawk_code', StringType(), True),
            StructField('icao_actype', StringType(), True),
            StructField('flight_number', StringType(), True),
            StructField('origin_airport_icao', StringType(), True),
            StructField('destination_airport_icao', StringType(), True),
            StructField('scheduled_departure_time_utc', TimestampType(), True),
            StructField('scheduled_departure_time_local', StringType(), True),
            StructField('scheduled_arrival_time_utc', TimestampType(), True),
            StructField('scheduled_arrival_time_local', StringType(), True),
            StructField('estimated_arrival_time_utc', TimestampType(), True),
            StructField('estimated_arrival_time_local', StringType(), True),
            StructField('tail_number', StringType(), True),
            StructField('ingestion_time', TimestampType(), True)])
        # self.time_profiles.__setattr__('ingestion', '_record_ingest_date')
        # self.time_profiles.__setattr__('aircraft', ['actual_out', 'adt', 'aat', 'actual_in', 'clock'])
        # self.time_profiles.__setattr__('id', )

    def spire(self, dt=None):
        if dt:
            prefix = 'Spire_'
            print(f'Processing {prefix}{dt}...')
            self.arguments.file_list = self.get_s3_file_list('sita-coe-ds-storage-spire', prefix=prefix,
                                                             post_prefix_list=dt)
        df = self.reader().csv(self.arguments.file_list, header=True, inferSchema=True)
        self.propose_schema(df)
        df = add_input_file_attributes(df, date_source='ingestion_time', time_source='ingestion_time')
        self.dataframes.__setattr__('airsafe', df)
        self.add_dataframes_options(partition_by=["record_ingest_date"], repartition=1,
                                    sort_within_partitions=['flight_number'], mode='append')
        self.add_dataframes_options(partition_by=["record_ingest_hour"])
        self.nested_run()
