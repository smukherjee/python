"""
FlightAware sources by metrics class
"""

from utils.features import MetricsFamily
from utils.map import Map
from registry import constants

METRICS = constants.METRICS

METRICS.Aerospace.Flight.source = Map({'default': 'der_fadooce_latest'})
METRICS.Aerospace.Aircraft.source = Map({'default': 'der_turnaround'})

SOURCES = Map(
    der_fadooce_latest='s3://sita-coe-ds-dev-v1/mart/der_fadooce_latest',
    der_turnaround='s3://sita-coe-ds-dev-v1/mart/der_turnaround'
)


class FlightAware(MetricsFamily):
    """
    FlightAware sources by metrics class
    """
    def __init__(self, *args, **kwargs):
        super().__init__(metrics_dict=constants.METRICS.Aerospace, data_type_dict=constants.DATA_TYPE_DICT,
                         source_dict=SOURCES, *args, **kwargs)
