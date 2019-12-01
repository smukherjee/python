"""
Script to run Spire pipeline
"""

from pipelines.spire.spire import Spire
from utils.date import date_range
from utils.pipeline import parse_pipeline_args


parse_pipeline_args(description='Process Spire Pipeline')

print(args)

out = Spire()
out.environment = args.environment
out.mart_folder = args.mart
out.save_to_object_map_path = args.save

dt_range = date_range(dt_start=args.dt_start, until=args.until, in_format=args.in_format, out_format=args.out_format)
for dt in dt_range:
    out.flightaware(dt)
