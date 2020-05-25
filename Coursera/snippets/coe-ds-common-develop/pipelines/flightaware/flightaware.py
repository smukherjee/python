"""
FlightAware Pipeline
"""

from copy import copy
from configs.common import PIPELINE_SCRIPT
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, DecimalType, IntegerType
from pyspark.sql.window import Window
from dataframe.utils import add_input_file_attributes, clone
from utils.map import Map
from utils.pipeline import Pipeline


class FlightAware(Pipeline):
    """
    FlightAware Pipeline
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline_order = [self.flightaware, self.der_rid_id, self.der_id_reg, self.der_latest_fadooce,
                               self.der_flightplan_next, self.der_combined_latest, self.der_turnaround]
        self.dependencies.__setattr__("der_rid_id", ['flightaware'])
        self.dependencies.__setattr__("der_id_reg", ['der_rid_id'])
        self.dependencies.__setattr__("der_latest_fadooce", ['der_id_reg'])
        self.dependencies.__setattr__("der_flightplan_next", ['der_latest_fadooce'])
        self.dependencies.__setattr__("der_combined_latest", ['der_flightplan_next'])
        self.dependencies.__setattr__("der_turnaround", ['der_combined_latest'])
        # self.process_groups.raw = Map(script=PIPELINE_SCRIPT, method_names=['flightaware'], core_instance_count=0,
        #                               instance_type="r5.xlarge", out_format='%Y-%m-%d_%H', by=1, freq='HOURLY',
        #                               auto_terminate=True)
        self.process_groups.raw = Map(script=PIPELINE_SCRIPT, method_names=['flightaware'], core_instance_count=0,
                                      instance_type="r5.xlarge", out_format='%Y-%m-%d', by=1, freq='DAILY',
                                      auto_terminate=True)
        self.process_groups.raw_stream = Map(script=PIPELINE_SCRIPT, method_names=['flightaware'],
                                             core_instance_count=0, instance_type="r5.xlarge",
                                             out_format='%Y-%m-%d_%H%M', by=1, freq='MINUTELY', auto_terminate=True)
        self.process_groups.derived1 = Map(script=PIPELINE_SCRIPT,
                                           method_names=['der_rid_id'], core_instance_count=0,
                                           instance_type="r5.xlarge", out_format='%Y-%m-%d', by=7, freq='DAILY',
                                           auto_terminate=True)
        self.process_groups.derived2 = Map(script=PIPELINE_SCRIPT,
                                           method_names=['der_id_reg', 'der_latest_fadooce'], core_instance_count=0,
                                           instance_type="r5.xlarge", out_format='%Y-%m-%d', by=7, freq='DAILY',
                                           auto_terminate=True)
        self.process_groups.derived3 = Map(script=PIPELINE_SCRIPT,
                                           method_names=['der_flightplan_next', 'der_combined_latest'],
                                           core_instance_count=0, instance_type="r5.xlarge", out_format='%Y-%m-%d',
                                           by=7, freq='DAILY', auto_terminate=True)
        self.process_groups.derived4 = Map(script=PIPELINE_SCRIPT,
                                           method_names=['der_turnaround'], core_instance_count=0,
                                           instance_type="r5.xlarge", out_format='%Y-%m-%d', by=7, freq='DAILY',
                                           auto_terminate=True)
        self.target_structures = ['arrival', 'cancellation', 'departure', 'extendedFlightInfo', 'flightplan',
                                  'offblock', 'onblock', 'position']
        self.abbreviations.__setattr__('arrival', 'arr')
        self.abbreviations.__setattr__('cancellation', 'can')
        self.abbreviations.__setattr__('departure', 'dep')
        self.abbreviations.__setattr__('extendedFlightInfo', 'ext')
        self.abbreviations.__setattr__('flightplan', 'fp')
        self.abbreviations.__setattr__('offblock', 'off')
        self.abbreviations.__setattr__('onblock', 'on')
        self.abbreviations.__setattr__('position', 'pos')
        # self.time_profiles.__setattr__('ingestion', '_record_ingest_date')
        # self.time_profiles.__setattr__('aircraft', ['actual_out', 'adt', 'aat', 'actual_in', 'clock'])
        # self.time_profiles.__setattr__('id', )
        self._FLIGHTAWARE_SCHEMA = StructType([
            StructField('aat', StringType(), True),
            StructField('actual_arrival_gate', StringType(), True),
            StructField('actual_arrival_terminal', StringType(), True),
            StructField('actual_departure_gate', StringType(), True),
            StructField('actual_departure_terminal', StringType(), True),
            StructField('actual_in', StringType(), True),
            StructField('actual_out', StringType(), True),
            StructField('adt', StringType(), True),
            StructField('air_ground', StringType(), True),
            StructField('aircrafttype', StringType(), True),
            StructField('alt', StringType(), True),
            StructField('altChange', StringType(), True),
            StructField('atcident', StringType(), True),
            StructField('baggage_claim', StringType(), True),
            StructField('clock', StringType(), True),
            StructField('dest', StringType(), True),
            StructField('edt', StringType(), True),
            StructField('estimated_arrival_gate', StringType(), True),
            StructField('estimated_departure_gate', StringType(), True),
            StructField('estimated_in', StringType(), True),
            StructField('estimated_out', StringType(), True),
            StructField('eta', StringType(), True),
            StructField('ete', StringType(), True),
            StructField('facility_hash', StringType(), True),
            StructField('facility_name', StringType(), True),
            StructField('fdt', StringType(), True),
            StructField('gs', StringType(), True),
            StructField('heading', StringType(), True),
            StructField('hexid', StringType(), True),
            StructField('id', StringType(), True),
            StructField('ident', StringType(), True),
            StructField('lat', StringType(), True),
            StructField('lon', StringType(), True),
            StructField('orig', StringType(), True),
            StructField('pitr', StringType(), True),
            StructField('reg', StringType(), True),
            StructField('route', StringType(), True),
            StructField('scheduled_arrival_terminal', StringType(), True),
            StructField('scheduled_departure_terminal', StringType(), True),
            StructField('scheduled_in', StringType(), True),
            StructField('scheduled_out', StringType(), True),
            StructField('speed', StringType(), True),
            StructField('squawk', StringType(), True),
            StructField('status', StringType(), True),
            StructField('synthetic', StringType(), True),
            StructField('timeType', StringType(), True),
            StructField('TrueCancel', StringType(), True),
            StructField('type', StringType(), True),
            StructField('updateType', StringType(), True),
            StructField('waypoints', ArrayType(StructType([
                StructField('lat', DecimalType(11, 7), True),
                StructField('lon', DecimalType(11, 7), True)]), True), True)])
        self.arguments.rid_partition = ['record_ingest_date']
        self.arguments.rid_partition_large = ['record_ingest_hour']
        # self.arguments.id_partition = ['id_date']
        self.arguments.fadooce_partition = ['edt_date']
        extra_columns = ['_input_file_name', '_record_ingest_date', 'record_ingest_time', 'record_ingest_date',
                         'record_ingest_hour', 'id_timestamp', 'id_date', 'id_hour']
        keep_id = ['facility_hash', 'id', 'ident', 'route', 'status', 'type', *extra_columns]
        self.columns_by_dataframe['arrival'] = [
            'aat', 'atcident', 'dest', 'edt', 'eta', 'ete', 'facility_hash', 'facility_name', 'id', 'ident', 'orig',
            'pitr', 'reg', 'synthetic', 'timeType', 'type', *extra_columns]
        self.columns_by_dataframe['cancellation'] = [
            'aircrafttype', 'alt', 'atcident', 'dest', 'edt', 'eta', 'ete', 'facility_hash', 'facility_name', 'fdt',
            'gs', 'hexid', 'id', 'ident', 'lat', 'lon', 'orig', 'pitr', 'reg', 'route', 'speed', 'status', 'trueCancel',
            'type', 'waypoints', *extra_columns]
        self.columns_by_dataframe['cancellation_waypoints'] = [*keep_id, 'waypoints_pos', 'waypoints']
        self.columns_by_dataframe['cancellation_waypoints_waypoints'] = [*keep_id, 'waypoints_pos', 'waypoints_lat',
                                                                         'waypoints_lon']
        self.columns_by_dataframe['departure'] = [
            'adt', 'aircrafttype', 'atcident', 'dest', 'edt', 'eta', 'ete', 'facility_hash', 'facility_name', 'id',
            'ident', 'orig', 'pitr', 'reg', 'synthetic', 'timeType', 'type', *extra_columns]
        self.columns_by_dataframe['extendedFlightInfo'] = [
            'actual_arrival_gate', 'actual_arrival_terminal', 'actual_departure_gate', 'actual_departure_terminal',
            'actual_in', 'actual_out', 'atcident', 'baggage_claim', 'dest', 'estimated_arrival_gate',
            'estimated_departure_gate', 'estimated_in', 'estimated_out', 'facility_hash', 'facility_name', 'id',
            'ident', 'orig', 'pitr', 'scheduled_arrival_terminal', 'scheduled_departure_terminal', 'scheduled_in',
            'scheduled_out', 'type', *extra_columns]
        self.columns_by_dataframe['flightplan'] = [
            'aircrafttype', 'alt', 'atcident', 'dest', 'edt', 'eta', 'ete', 'facility_hash', 'facility_name', 'fdt',
            'gs', 'hexid', 'id', 'ident', 'lat', 'lon', 'orig', 'pitr', 'reg', 'route', 'speed', 'status', 'trueCancel',
            'type', 'waypoints', *extra_columns]
        self.columns_by_dataframe['flightplan_waypoints'] = [*keep_id, 'waypoints_pos', 'waypoints']
        self.columns_by_dataframe['flightplan_waypoints_waypoints'] = [*keep_id, 'waypoints_pos', 'waypoints_lat',
                                                                       'waypoints_lon']
        self.columns_by_dataframe['offblock'] = [
            'clock', 'dest', 'facility_hash', 'facility_name', 'id', 'ident', 'orig', 'pitr', 'type', *extra_columns]
        self.columns_by_dataframe['onblock'] = copy(self.columns_by_dataframe['offblock'])
        self.columns_by_dataframe['position'] = [
            'air_ground', 'aircrafttype', 'alt', 'altChange', 'atcident', 'clock', 'dest', 'edt', 'facility_hash',
            'facility_name', 'gs', 'heading', 'hexid', 'id', 'ident', 'lat', 'lon', 'orig', 'pitr', 'reg', 'speed',
            'squawk', 'type', 'updateType', *extra_columns]

    def flightaware(self, dt=None):
        prefix = f'FlightAwareFirehose_'
        if dt:
            print(f'Processing {prefix}{dt}...')
            self.arguments.file_list = self.get_s3_file_list('flightawarev7', prefix=prefix, post_prefix_list=dt)
        if not self.arguments.file_list and not self.save_to_object_map_path:
            Warning('Not saving back, use default file_list to produce logical objects.')
            self.arguments.file_list = self.get_s3_file_list('flightawarev7', prefix=prefix,
                                                             post_prefix_list=['2019-09-15_0000'])
        df = self.reader().json(self.arguments.file_list)
        self.propose_schema(df)
        df = add_input_file_attributes(df, rid_regex='\\d{1,4}-\\d{1,2}-\\d{1,2}_\\d{1,4}',
                                       rid_format='yyyy-MM-dd_HHmm')
        # Retrieve epoch timestamp from the id column, which we use as a partitioning key for all structures.
        # NOTE: If we were using Databricks delta, we would be able to leverage Z-ordering instead of this workaround.
        df = df.withColumn('id_timestamp',
                           f.regexp_replace(
                               f.regexp_replace('id', '^[^-]*-', ''),
                               '-.*', '').cast('long').cast('timestamp'))
        df = df.withColumn('id_date', f.to_date('id_timestamp'))
        df = df.withColumn('id_hour', f.date_format('id_timestamp', 'HH'))

        self.structure_split(df, structure_columns=['type'], target_structures=self.target_structures)
        for timestamp_column in ['aat', 'actual_in', 'actual_out', 'adt', 'clock', 'edt', 'eta', 'estimated_in',
                                 'estimated_out', 'fdt', 'pitr']:
            self.apply_transformations(timestamp_column, [timestamp_column],
                                       f.col(timestamp_column).cast('long').cast('timestamp'))
        coord_columns = ['lat', 'lon']
        for coord_column in coord_columns:
            self.apply_transformations(coord_column, coord_columns, f.col(coord_column).cast(DecimalType(11, 7)))
        int_columns = ['alt', 'gs', 'heading', 'speed', 'ete']
        for int_column in int_columns:
            self.apply_transformations(int_column, int_columns, f.col(int_column).cast(IntegerType()))

        self.add_dataframes_options(partition_by=self.arguments.rid_partition, repartition=1,
                                    sort_within_partitions=['id'], mode='append')
        large = ['position', 'flightplan_waypoints_waypoints']
        self.add_dataframes_options(partition_by=self.arguments.rid_partition_large, mode='append',
                                    dataframe_names=large)
        self.dataframes.cancellation = self.dataframes.cancellation.drop('waypoints')
        self.dataframes.flightplan = self.dataframes.flightplan.drop('waypoints')
        self.dataframes_omit = ['cancellation_waypoints', 'flightplan_waypoints']
        self.nested_run()

    def der_rid_id(self, name='der_rid_id', dt=None):
        rid_id = self.load('flightplan')
        if dt:
            rid_id = rid_id.filter(f.col('record_ingest_date').isin(dt))
        keys = [*self.arguments.rid_partition, *self.arguments.rid_partition_large, *self.arguments.fadooce_partition]
        rid_id = rid_id.withColumn('edt_date', f.to_date('edt')).select(*keys, 'id').distinct()

        self.dataframes[name] = rid_id
        self.add_dataframes_options(partition_by=self.arguments.fadooce_partition, repartition=1, mode='append',
                                    dataframe_names=name, overwrite=True)
        self.nested_run(dataframe_names=name)

    def der_id_reg(self, dt=None):
        order_by = [f.col(col).desc() for col in self.arguments.rid_partition]
        order_by.append(f.col('_record_ingest_date').desc())
        rid_id = self.load('der_rid_id')
        load_filter = None
        if dt:
            rid_id = rid_id.filter(f.col('edt_date') >= f.lit(min(dt)))
            load_filter = (f.date_add('record_ingest_date', 7) > f.lit(min(dt))) &\
                          (f.date_add('record_ingest_date', -7) < f.lit(max(dt)))
        f.broadcast(rid_id)
        latest = self.get_latest_records(partition_by=['id'], order_by=order_by, join_df=rid_id,
                                         load_filter=load_filter, filter=f.col('reg').isNotNull(),
                                         select=['id', 'reg', *self.arguments.fadooce_partition, 'id_timestamp',
                                                 '_record_ingest_date'], dataframe_names='flightplan',
                                         prefix='der_', suffix='_reg')
        rid_id.unpersist()
        if dt:
            self.filter_values(f.col('edt_date').isin(dt), dataframe_names=latest)
        self.add_dataframes_options(partition_by=self.arguments.fadooce_partition, repartition=1,
                                    sort_within_partitions=['record_ingest_date', 'id'], mode='append',
                                    dataframe_names=latest, overwrite=True)
        self.nested_run(dataframe_names=latest)

    def der_latest_fadooce(self, dataframe_names=None, dt=None):
        if not dataframe_names:
            dataframe_names = ['flightplan', 'arrival', 'departure', 'onblock', 'offblock', 'cancellation',
                               'extendedFlightInfo']
        id_reg = self.load('der_flightplan_reg').drop('_record_ingest_date')
        load_filter = None
        if dt:
            id_reg = id_reg.filter(f.col('edt_date').isin(dt))
            load_filter = (f.date_add('record_ingest_date', 7) > f.lit(min(dt))) & \
                          (f.date_add('record_ingest_date', -7) < f.lit(max(dt)))
        f.broadcast(id_reg)
        latest = self.get_latest_records(partition_by=['id'],
                                         order_by=[*[f.col(col).desc() for col in self.arguments.rid_partition],
                                                   f.col('_record_ingest_date').desc()], load_filter=load_filter,
                                         join_df=id_reg, how='left', dataframe_names=dataframe_names)
        id_reg.unpersist()
        self.add_dataframes_options(partition_by=self.arguments.fadooce_partition, repartition=1,
                                    sort_within_partitions=['reg'], mode='append', dataframe_names=latest,
                                    overwrite=True)
        self.nested_run(dataframe_names=latest)

    def der_flightplan_next(self, name='der_flightplan_next', dt=None):
        key_fields = [*self.arguments.fadooce_partition, 'id', 'reg', 'orig', 'dest']
        fp = self.load('der_flightplan_latest').select(*key_fields, 'edt', 'eta')
        fp_next_can = fp
        dep = self.load('der_departure_latest').select(*key_fields, 'adt')
        dep_next_can = dep
        n_days = 4
        if dt:
            cond = (f.col('edt_date') >= f.lit(min(dt))) & (f.date_add('edt_date', -n_days) < f.lit(max(dt)))
            fp_next_can = fp.filter(cond)
            fp = fp_next_can.filter(f.col('edt_date').isin(dt))
            dep_next_can = dep.filter(cond)
            dep = dep_next_can.filter(f.col('edt_date').isin(dt))
        fp_left = fp.join(dep, on=key_fields, how='left')
        fp_can = fp_next_can.join(dep_next_can, on=key_fields, how='left')
        key_fields.remove('reg')
        fp_can = fp_can.withColumn('edt_date_orig', f.col('edt_date'))
        for col in [*key_fields, 'edt_date', 'edt', 'eta', 'adt']:
            fp_can = fp_can.withColumnRenamed(col, f'{col}_next')
        window = Window.partitionBy([*self.arguments.fadooce_partition, 'id', 'reg']).orderBy('edt_next')
        cond = [fp_left['edt_date'] == fp_can['edt_date_orig'], fp_left['reg'] == fp_can['reg']]
        df_next = fp_left.join(fp_can, on=cond, how="left"). \
            drop(fp_can['reg']).drop(fp_can['edt_date_orig']). \
            filter(f.col('edt_next') > f.col('edt')). \
            filter(f.col('adt').isNotNull()). \
            withColumn('__row_number', f.row_number().over(window)). \
            filter(f.col('__row_number') == 1). \
            withColumn('__is_valid', f.col('orig_next') == f.col('dest')). \
            drop('__row_number', 'orig', 'dest', 'edt', 'eta', 'adt')
        df_next.cache()
        name_invalid = f'{name}_invalid'
        self.dataframes[name] = df_next.filter(f.col('__is_valid')).drop('__is_valid')
        self.dataframes[name_invalid] = df_next.filter(~f.col('__is_valid')).drop('__is_valid')
        df_next.unpersist()

        self.dataframes_options.__setattr__(name, self.dataframes_options.__getattr__('der_flightplan_latest'))
        self.dataframes_options.__setattr__(name_invalid, self.dataframes_options.__getattr__(name))

        self.nested_run(dataframe_names=[name, name_invalid])


    def der_combined_latest(self, name='der_fadooce_latest', dataframe_names=None, dt=None):
        if not dataframe_names:
            dataframe_names = ['arrival', 'departure', 'onblock', 'offblock', 'cancellation', 'extendedFlightInfo']
            dataframe_names = [f'der_{name}_latest' for name in dataframe_names]
            dataframe_names = ['der_flightplan_latest', 'der_flightplan_next', *dataframe_names]
        load_filter = None
        if dt:
            load_filter = f.col('edt_date').isin(dt)
        combined = self.join_sources(id_columns=[*self.arguments.fadooce_partition, 'id'],
                                     dataframe_names=dataframe_names, combined_name=name, how='left',
                                     load_filter=load_filter)
        self.nested_run(dataframe_names=combined)

    def der_turnaround(self, name='der_turnaround', dt=None):
        fadooce = self.load('der_fadooce_latest').filter(f.col('reg').isNotNull())
        off_columns = self.load('der_offblock_latest').columns
        dep_columns = self.load('der_departure_latest').columns
        ext_columns = self.load('der_extendedFlightInfo_latest').columns
        off_abbr = self.abbreviations.der_offblock_latest
        # on_abbr = self.abbreviations.der_onblock_latest
        # arr_abbr = self.abbreviations.der_arrival_latest
        dep_abbr = self.abbreviations.der_departure_latest
        ext_abbr = self.abbreviations.der_extendedFlightInfo_latest
        off_columns = [f'{col}_{off_abbr}' for col in off_columns]
        off_columns = [col for col in off_columns if col in fadooce.columns]
        dep_columns = [f'{col}_{dep_abbr}' for col in dep_columns]
        dep_columns = [col for col in dep_columns if col in fadooce.columns]
        ext_columns_dep = [f'{col}_{ext_abbr}' for col in ext_columns
                           if col.find('_departure_') > -1 or col.find('_out') > -1]
        ext_columns_dep = [col for col in ext_columns_dep if col in fadooce.columns]
        fadooce_on = fadooce.drop(*off_columns, *dep_columns, *ext_columns_dep)
        fadooce_off = fadooce.select(*self.arguments.fadooce_partition, 'id', *off_columns, *dep_columns,
                                     *ext_columns_dep)
        if dt:
            fadooce_on = fadooce_on.filter(f.col('edt_date').isin(dt))
            fadooce_off = fadooce_off.filter(f.col('edt_date') >= f.lit(min(dt)))
        # Clone dataframes to workaround unresolved references.
        fadooce_on = clone(fadooce_on)
        fadooce_off = clone(fadooce_off)

        id_keys = [*self.arguments.fadooce_partition, 'id']
        cond = [fadooce_on[f'{col}_next'] == fadooce_off[col] for col in id_keys]
        on_off = fadooce_on.join(fadooce_off, on=cond, how="left")
        for col in id_keys:
            on_off = on_off.drop(fadooce_off[col])

        # on_off = on_off.withColumn('turnaround_minutes',
        #                            ((f.col('clock_off').cast('long') - f.col('clock_on').cast('long')) / 60).cast(
        #                                'decimal(8,1)'))

        self.dataframes.__setattr__(name, on_off)
        self.dataframes_options.__setattr__(name, self.dataframes_options.__getattr__('der_flightplan_latest'))
        self.nested_run(dataframe_names=name)
