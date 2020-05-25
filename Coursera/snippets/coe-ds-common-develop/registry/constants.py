"""
Constants for registry metrics
"""
from utils.map import Map
from pyspark.sql.types import StringType, DecimalType
METRICS = Map()

METRICS.Aerospace = Map(
    Flight=Map(metrics=['id', 'clock']),
    Aircraft=Map(metrics=['reg', 'clock_on', 'clock_off', 'turnaround'],
                 logic=Map(turnaround="(f.col('clock_off').cast('long') - f.col('clock_on').cast('long')) / 60"))
)

DATA_TYPE_DICT = Map()

DATA_TYPE_DICT.id = StringType()
DATA_TYPE_DICT.reg = StringType()
DATA_TYPE_DICT.lat = DecimalType(11, 7)
DATA_TYPE_DICT.lon = DATA_TYPE_DICT.lat
DATA_TYPE_DICT.speed = DecimalType(12, 5)
DATA_TYPE_DICT.heading = DecimalType(15, 9)


# Timestamps
for col in ['clock', 'clock_on', 'clock_off']:
    DATA_TYPE_DICT.__setattr__(col, 'timestamp')

DATA_TYPE_DICT.__setattr__('turnaround', 'decimal(8,1)')
