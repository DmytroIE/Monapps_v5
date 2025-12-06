import logging
import traceback
from itertools import islice
from collections.abc import Iterable

from django.db import transaction
from django.conf import settings

from apps.datastreams.models import Datastream
from apps.devices.models import Device
from apps.dsreadings.models import (
    DsReading,
    UnusedDsReading,
    InvalidDsReading,
    NonRocDsReading,
    NoDataMarker,
    UnusedNoDataMarker,
)
from utils.dsr_utils import create_ds_readings, create_nodata_markers
from utils.ts_utils import create_now_ts_ms
from utils.update_utils import set_attr_if_cond, enqueue_update
from utils.alarm_utils import update_alarm_map, at_least_one_alarm_in
from utils.sequnce_utils import find_max_ts
from services.device_log import add_to_device_log
from common.constants import HealthGrades, VariableTypes, DataAggTypes

logger = logging.getLogger("#raw_data_proc")


class RawDataProcessor:
    def __init__(self, dev_ui: str, payload: dict):
        self.dev_ui = dev_ui
        self.payload = payload
        self.int_key_payload = {}

    def execute(self):

        if not self.discover_device():
            logger.error(f"Cannot discover device {self.dev_ui}")
            return

        if not self.condition_payload():
            logger.error("No valid timestamps in the payload")
            return
        try:
            with transaction.atomic():
                self.prepare_for_processing()
                self.process_payload()
                self.process_after_cycle()
        except Exception:
            # add_to_alarm_log("ERROR", "Error while processing a message", instance="MQTT Sub")
            logger.error(f"Error while processing a message: {traceback.format_exc(-1)}")

    def discover_device(self):
        try:
            self.dev = Device.objects.get(dev_ui=self.dev_ui)
            return True
        except (Device.DoesNotExist, Device.MultipleObjectsReturned):
            return False

    def condition_payload(self):
        self.replace_str_tss_with_ints()
        if len(self.int_key_payload) == 0:
            return False
        self.int_key_payload = dict(sorted(self.int_key_payload.items()))  # sort by timestamps
        return True

    def replace_str_tss_with_ints(self):
        for k, v in self.payload.items():
            try:
                ts = int(k)
            except ValueError as e:
                logger.error(f"Cannot convert {k} to a timestamp, {e}")
            else:
                self.int_key_payload[ts] = v

    def prepare_for_processing(self):
        self.dev = Device.objects.select_for_update().get(dev_ui=self.dev.dev_ui)
        ds_qs = self.dev.datastreams.filter(is_enabled=True).select_for_update()  # get ACTIVE datastreams only
        self.ds_map = {ds.name: ds for ds in ds_qs}
        self.nd_marker_map = {ds.name: set() for ds in ds_qs}
        self.ds_reading_map = {ds.name: {} for ds in ds_qs}

    def process_payload(self):
        for ts, row in self.int_key_payload.items():
            self.needing_nd_marker_dss = set()
            self.at_least_one_ds_has_no_errors_and_has_value = False
            # process the datastreams
            for ds in self.ds_map.values():
                if (ds_row := row.get(ds.name)) is None:
                    ds_row = {}  # a plug
                # 'process_ds_payload' should be executed even if there is no data
                # for the datastream in the row
                self.process_ds_payload(ds, ts, ds_row)

            # Process the device
            self.process_dev_payload(self.dev, ts, row)

    def process_ds_payload(self, ds: Datastream, ts: int, ds_row: dict):
        # process values
        has_value = False
        new_value = ds_row.get("v")
        if new_value is not None and isinstance(new_value, (int, float)):
            # add value to the array to be saved later
            self.ds_reading_map[ds.name][ts] = new_value
            has_value = True

        # process errors
        error_dict = ds_row.get("e")
        # even if 'error_dict_for_ts' is None, the alarm map will be processed
        # to ensure 'out' statuses proper assigment
        upd_error_map, is_nd_marker_needed = update_alarm_map(
            ds, error_dict, ts, "errors", has_value, add_to_log=add_to_device_log
        )

        set_attr_if_cond(upd_error_map, "!=", ds, "errors")

        if is_nd_marker_needed:
            self.needing_nd_marker_dss.add(ds.name)
        else:
            if has_value:
                self.at_least_one_ds_has_no_errors_and_has_value = True

        # process warnings
        warning_dict = ds_row.get("w")
        upd_warning_map, _ = update_alarm_map(ds, warning_dict, ts, "warnings", add_to_log=add_to_device_log)
        set_attr_if_cond(upd_warning_map, "!=", ds, "warnings")

        # process infos
        infos_for_ts = ds_row.get("i")
        if infos_for_ts is not None and isinstance(infos_for_ts, Iterable):
            for info_str in infos_for_ts:
                add_to_device_log("INFO", info_str, ts, ds, "")

    def process_dev_payload(self, dev: Device, ts: int, row: dict):
        # process errors
        error_dict = row.get("e")
        upd_error_map, is_nd_marker_needed = update_alarm_map(
            dev,
            error_dict,
            ts,
            "errors",
            self.at_least_one_ds_has_no_errors_and_has_value,
            add_to_log=add_to_device_log,
        )
        set_attr_if_cond(upd_error_map, "!=", dev, "errors")

        if is_nd_marker_needed:
            self.needing_nd_marker_dss.update(self.ds_map.keys())  # on device error all datastreams acquire nd markers

        # process device warnings
        warning_dict = row.get("w")
        upd_warning_map, _ = update_alarm_map(dev, warning_dict, ts, "warnings", add_to_log=add_to_device_log)
        set_attr_if_cond(upd_warning_map, "!=", dev, "warnings")

        # process device infos
        infos = row.get("i")
        if infos is not None and isinstance(infos, Iterable):
            for info_str in infos:
                add_to_device_log("INFO", info_str, ts, dev)

        # add nd markers to the array to be saved later
        for ds_name in self.needing_nd_marker_dss:
            self.nd_marker_map[ds_name].add(ts)

    def process_after_cycle(self):
        for ds in self.ds_map.values():
            self.process_ds_after_cycle(ds)

        self.process_dev_after_cycle(self.dev)

    def process_ds_after_cycle(self, ds: Datastream):
        # define ds health
        at_least_one_error_in = at_least_one_alarm_in(ds.errors)
        at_least_one_warning_in = at_least_one_alarm_in(ds.warnings)

        msg_health = HealthGrades.UNDEFINED
        if at_least_one_error_in:
            msg_health = HealthGrades.ERROR
        elif at_least_one_warning_in:
            msg_health = HealthGrades.WARNING

        now_ts = create_now_ts_ms()

        if set_attr_if_cond(msg_health, "!=", ds, "msg_health"):

            health = max(ds.msg_health, ds.nd_health)

            if set_attr_if_cond(health, "!=", ds, "health"):
                # as the ds health changed it is necessary to enqueue the parent device update
                enqueue_update(self.dev, now_ts)

        # create nd markers
        if not ds.is_rbe or (
            ds.data_type.var_type == VariableTypes.CONTINUOUS and ds.data_type.agg_type == DataAggTypes.AVG
        ):
            nd_markers = []
            unused_nd_markers = []
            # no sense in creating nodata markers for this type of data or if a ds is not RBE
        else:
            nd_markers, unused_nd_markers = create_nodata_markers(self.nd_marker_map[ds.name], ds, now_ts)

        # create ds readings
        ds_readings, unused_ds_readings, invalid_ds_readings, non_roc_ds_readings = create_ds_readings(
            self.ds_reading_map[ds.name], ds, now_ts
        )

        # update 'ts_to_start_with' and 'last_valid_reading_ts'
        ts_to_start_with = max(find_max_ts(ds_readings), find_max_ts(nd_markers))
        set_attr_if_cond(ts_to_start_with, ">", ds, "ts_to_start_with")

        last_valid_reading_ts = find_max_ts(ds_readings)  # ds_readings - only valid readings
        set_attr_if_cond(last_valid_reading_ts, ">", ds, "last_valid_reading_ts")

        # for periodic datastreams plan health recalculation right away
        if ds.time_update is not None:
            ds.health_next_eval_ts = now_ts + settings.TIME_DS_HEALTH_EVAL_MS
            ds.update_fields.add("health_next_eval_ts")

        # finally, save the datastream and readings
        ds.save(update_fields=ds.update_fields)

        t = (
            (ds_readings, DsReading),
            (unused_ds_readings, UnusedDsReading),
            (invalid_ds_readings, InvalidDsReading),
            (non_roc_ds_readings, NonRocDsReading),
            (nd_markers, NoDataMarker),
            (unused_nd_markers, UnusedNoDataMarker),
        )
        for objects, model in t:
            batch_size = 100
            for i in range(0, len(objects), batch_size):
                if (batch := objects[i:i + batch_size]):
                    # ignore_conflicts=True will skip saving the object if it already exists
                    # TODO: how to track such "unsaved" objects?
                    model.objects.bulk_create(batch, batch_size=len(batch), ignore_conflicts=True)
            logger.debug(f"Saved {len(objects)} {model.__name__}")

    def process_dev_after_cycle(self, dev: Device):
        # define device health
        at_least_one_error_in = at_least_one_alarm_in(dev.errors)
        at_least_one_warning_in = at_least_one_alarm_in(dev.warnings)

        msg_health = HealthGrades.UNDEFINED

        if at_least_one_error_in:
            msg_health = HealthGrades.ERROR
        elif at_least_one_warning_in:
            msg_health = HealthGrades.WARNING

        if not set_attr_if_cond(msg_health, "!=", dev, "msg_health"):
            return

        enqueue_update(dev, create_now_ts_ms())

        dev.save(update_fields=dev.update_fields)
