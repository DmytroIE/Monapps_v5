import logging
from collections.abc import Iterable

from apps.datastreams.models import Datastream
from apps.dsreadings.models import (
    DsReading,
    UnusedDsReading,
    InvalidDsReading,
    NonRocDsReading,
    NoDataMarker,
    UnusedNoDataMarker,
)
from common.constants import DataAggTypes, VariableTypes

logger = logging.getLogger("#dsr_utils")


def create_ds_readings(
    pairs_ts_value: dict[int, float | int], ds: Datastream, now: int
) -> tuple[list[DsReading], list[UnusedDsReading], list[InvalidDsReading], list[NonRocDsReading]]:

    ds_readings, unused_ds_readings = sort_unused_ds_readings(pairs_ts_value, ds, now)
    ds_readings, invalid_ds_readings = validate_ds_readings(ds_readings, ds)
    non_roc_ds_readings = []
    if ds.data_type.agg_type == DataAggTypes.AVG and ds.data_type.var_type == VariableTypes.CONTINUOUS:
        ds_readings, non_roc_ds_readings = roc_filter_ds_readings(ds_readings, ds)

    if len(ds_readings) > 0:
        logger.debug(f"Created {len(ds_readings)} ds_readings")
    if len(unused_ds_readings) > 0:
        logger.debug(f"Created {len(unused_ds_readings)} unused ds_readings")
    if len(invalid_ds_readings) > 0:
        logger.debug(f"Created {len(invalid_ds_readings)} invalid ds_readings")
    if len(non_roc_ds_readings) > 0:
        logger.debug(f"Created {len(non_roc_ds_readings)} non_roc ds_readings")

    return ds_readings, unused_ds_readings, invalid_ds_readings, non_roc_ds_readings


def create_nodata_markers(
    tss: Iterable[int], ds: Datastream, now: int
) -> tuple[list[NoDataMarker], list[UnusedNoDataMarker]]:

    nd_markers = []
    unused_nd_markers = []

    from_ts = ds.ts_to_start_with

    for ts in tss:
        if ts > from_ts and ts < now:
            nd_markers.append(NoDataMarker(time=ts, datastream=ds))
        else:
            unused_nd_markers.append(UnusedNoDataMarker(time=ts, datastream=ds))

    if len(nd_markers) > 0:
        logger.debug(f"Created {len(nd_markers)} nd_markers.")
    if len(unused_nd_markers) > 0:
        logger.debug(f"Created {len(unused_nd_markers)} unused nd_markers.")

    return nd_markers, unused_nd_markers


def sort_unused_ds_readings(
    pairs_ts_value: dict[int, float | int], ds: Datastream, now: int
) -> tuple[list[DsReading], list[UnusedDsReading]]:

    from_ts = ds.ts_to_start_with

    used_ds_readings = []
    unused_ds_readings = []

    for ts, val in pairs_ts_value.items():
        if ts > from_ts and ts < now:
            dsr = DsReading(time=ts, value=val, datastream=ds)
            used_ds_readings.append(dsr)
        else:
            dsr = UnusedDsReading(time=ts, value=val, datastream=ds)
            unused_ds_readings.append(dsr)

    return used_ds_readings, unused_ds_readings


def validate_ds_readings(
    ds_readings: list[DsReading], ds: Datastream
) -> tuple[list[DsReading], list[InvalidDsReading]]:

    valid_ds_readings = []
    invalid_ds_readings = []
    for r in ds_readings:
        if r.value <= ds.max_plausible_value and r.value >= ds.min_plausible_value:
            valid_ds_readings.append(r)
        else:
            ir = InvalidDsReading(time=r.time, value=r.value, datastream=r.datastream)
            invalid_ds_readings.append(ir)

    return valid_ds_readings, invalid_ds_readings


def roc_filter_ds_readings(
    ds_readings: list[DsReading], ds: Datastream
) -> tuple[list[DsReading], list[NonRocDsReading]]:

    sorted_ds_readings = sorted(ds_readings, key=lambda r: r.time)

    proc_ds_readings = []
    non_proc_ds_readings = []
    if len(sorted_ds_readings) > 0:
        base_point = (
            DsReading.objects.filter(datastream__id=ds.pk, time__lt=sorted_ds_readings[0].time).order_by("time").last()
        )

        if base_point is None:
            prev_filt_val = sorted_ds_readings[0].value
            prev_filt_ts = sorted_ds_readings[0].time
        else:
            prev_filt_val = base_point.value
            prev_filt_ts = base_point.time

        for r in sorted_ds_readings:
            sign = 1
            if r.value - prev_filt_val < 0:
                sign = -1
            limit_value = prev_filt_val + sign * ds.max_rate_of_change * (r.time - prev_filt_ts) / 1000
            if (sign > 0 and limit_value < r.value) or (sign < 0 and limit_value > r.value):
                npr = NonRocDsReading(time=r.time, value=r.value, datastream=r.datastream)
                non_proc_ds_readings.append(npr)
                # then change the value in the initial ds reading
                r.value = limit_value
            proc_ds_readings.append(r)

            prev_filt_val = r.value
            prev_filt_ts = r.time

    return proc_ds_readings, non_proc_ds_readings
