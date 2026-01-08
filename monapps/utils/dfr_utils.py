import logging
from scipy.interpolate import PchipInterpolator
from typing import Sequence

from apps.datafeeds.models import Datafeed
from apps.dsreadings.models import DsReading, NoDataMarker
from apps.dfreadings.models import DfReading

from common.complex_types import IndDfReadingMap
from common.constants import DataAggTypes, NotToUseDfrTypes
from utils.ts_utils import ceil_timestamp, create_grid


logger = logging.getLogger("#dfr_utils")


def find_average(ds_readings: list[DsReading]) -> float | None:

    if len(ds_readings) == 0:
        return None
    sum = 0
    length = len(ds_readings)
    for r in ds_readings:
        sum += r.value
    avgd_value = sum / length
    return avgd_value


def find_sum(ds_readings: list[DsReading]) -> float | int | None:

    if len(ds_readings) == 0:
        return None
    sum = 0
    for r in ds_readings:
        sum += r.value
    return sum


def find_last_value(ds_readings: list[DsReading]) -> float | int | None:

    if len(ds_readings) == 0:
        return None
    ds_readings.sort(key=lambda r: r.time)
    last_value = ds_readings[-1].value
    return last_value


agg_map = {DataAggTypes.AVG: find_average, DataAggTypes.SUM: find_sum, DataAggTypes.LAST: find_last_value}


def resample_ds_readings(
    sorted_ds_readings: list[DsReading],
    df: Datafeed,
    time_resample: int,
    agg_type: DataAggTypes,
) -> IndDfReadingMap:
    """
    A generic function, can be used with different aggregation functions.
    """

    df_reading_map = {}
    agg_func = agg_map[agg_type]

    last_df_reading_rts = 0
    for r in sorted_ds_readings:
        rts = ceil_timestamp(r.time, time_resample)
        if rts not in df_reading_map:
            df_reading_map[rts] = []

        df_reading_map[rts].append(r)
        last_df_reading_rts = rts

    for rts in df_reading_map:
        agg_value = agg_func(df_reading_map[rts])
        if agg_value is not None:
            dfr = DfReading(time=rts, value=agg_value, datafeed=df, restored=False)
            df_reading_map[rts] = dfr
            # injection of 'not_to_use' property
            if rts == last_df_reading_rts:
                dfr.not_to_use = NotToUseDfrTypes.UNCLOSED

    return df_reading_map


def resample_and_augment_ds_readings(
    sorted_dsrs_and_ndms: Sequence[DsReading | NoDataMarker],
    df: Datafeed,
    time_resample: int,
    start_rts: int,
    end_rts: int,
    agg_type: DataAggTypes,
    is_nd_period_open: bool,
    dfr_at_start_ts: DfReading | None = None,
) -> IndDfReadingMap:

    if start_rts >= end_rts:
        return {}

    df_reading_map = {}
    agg_func = agg_map[agg_type]

    for r in sorted_dsrs_and_ndms:
        rts = ceil_timestamp(r.time, time_resample)
        if rts not in df_reading_map:
            df_reading_map[rts] = []

        df_reading_map[rts].append(r)

    # also, we need the previous value to continue the series
    # add this dfr temporarily, it will be removed at the end of the function
    if dfr_at_start_ts is not None:
        df_reading_map[start_rts] = dfr_at_start_ts

    grid = create_grid(start_rts + time_resample, end_rts, time_resample)

    for rts in grid:
        arr = df_reading_map.get(rts, None)  # 'arr' is already sorted by time
        if arr is not None:  # if 'arr' is not None, then it contains at least one item: dsr or ndm
            if isinstance(arr[-1], NoDataMarker):  # if the last item in 'arr' is NoDataMarker
                is_nd_period_open = True
            else:
                is_nd_period_open = False  # new ds readings after an nd_marker "destroy" nodata period

            new_arr = [r for r in arr if isinstance(r, DsReading)]
            agg_value = agg_func(new_arr)  # if 'new_arr' is empty, 'agg_func' will return None

            if agg_value is not None:
                dfr = DfReading(time=rts, value=agg_value, datafeed=df, restored=False)
                df_reading_map[rts] = dfr
            else:  # only nodata marker in 'arr'
                del df_reading_map[rts]

        else:
            if not is_nd_period_open:
                dfr = None
                if agg_type == DataAggTypes.SUM:
                    dfr = DfReading(time=rts, value=0, datafeed=df, restored=True)
                elif agg_type == DataAggTypes.LAST:
                    prev_dfr = df_reading_map.get(rts - time_resample, None)
                    if prev_dfr is not None:  # may be None at 'start_rts'
                        dfr = DfReading(time=rts, value=prev_dfr.value, datafeed=df, restored=True)
                else:
                    raise ValueError(f"Unknown augmentation type for {agg_type}")
                if dfr is not None:
                    df_reading_map[rts] = dfr

    # remove the temporarily added dfr from the previous period
    if df_reading_map.get(start_rts, None) is not None:
        del df_reading_map[start_rts]

    # we assume that the last bin is always unclosed
    if df_reading_map.get(end_rts, None) is not None:
        del df_reading_map[end_rts]

    return df_reading_map


# For 'continuous + AVG' datastreams
def restore_continuous_avg(
    df_reading_map: IndDfReadingMap,
    df: Datafeed,
    time_resample: int,
    time_change: int,
    start_rts: int,
    last_nat_dfrs_from_prev_period: list[DfReading],
) -> IndDfReadingMap:

    if len(df_reading_map) == 0:  # nothing to restore
        return df_reading_map

    sorted_df_readings = sorted(df_reading_map.values(), key=lambda x: x.time)

    for dfr in sorted_df_readings:  # NOTE: just in case, probably not necessary at all
        dfr.not_to_use = None

    # add some readings 'from the past' to have enough readings for interpolation
    next_rts = sorted_df_readings[0].time
    i = len(last_nat_dfrs_from_prev_period) - 1
    if i >= 0:
        while i >= 0:
            if next_rts - last_nat_dfrs_from_prev_period[i].time <= time_change:
                # TODO: prepending operation,
                # maybe not the best solution from the performance point of view
                sorted_df_readings.insert(0, last_nat_dfrs_from_prev_period[i])
            else:
                break
            next_rts = last_nat_dfrs_from_prev_period[i].time
            i -= 1

    # first it is necessary to obtain the clusters of points to build splines
    clusters = []
    cluster = {sorted_df_readings[0].time: sorted_df_readings[0]}  # initialize the first cluster with the first point
    length = len(sorted_df_readings)
    i = 1
    while i <= length:
        if i < length and sorted_df_readings[i].time - sorted_df_readings[i - 1].time <= time_change:
            cluster[sorted_df_readings[i].time] = sorted_df_readings[i]
            i += 1
        else:
            clusters.append(cluster)
            if i == length:
                break
            else:
                cluster = {sorted_df_readings[i].time: sorted_df_readings[i]}
                i += 1

    # after this procedure 'clusters' look like
    # [{1723698300000: <31>, 1723698360000:<30>}, {1723698720000: <30>, 1723698780000:<29>}, ...],
    # i.e. groups of DfReadings, the distance between groups is >= 'time_change'
    # at least one cluster with one reading will be created
    # the last cluster is always "not closed"

    part_grid = None
    spline = None
    cl_rtimestamps = None  # these are the timestamps of a cluster's 'native' points
    cl_values = None  # these are the values of a cluster's 'native' points
    restored_values = None
    new_df_reading_map = df_reading_map.copy()

    # process all the clusters except the last one
    length = len(clusters)
    if length > 1:  # the last cluster will be processed separately
        for i in range(length - 1):
            cluster = clusters[i]
            cl_rtimestamps = list(cluster.keys())
            cl_rtimestamps.sort()

            if len(cl_rtimestamps) > 1:  # more than one point in the cluster

                cl_values = [cluster[rts].value for rts in cl_rtimestamps]

                spline = PchipInterpolator(cl_rtimestamps, cl_values)
                part_grid = create_grid(cl_rtimestamps[0], cl_rtimestamps[-1], time_resample)
                restored_values = spline(part_grid)

                for rts, val in zip(part_grid, restored_values):
                    if rts not in cl_rtimestamps:
                        cluster[rts] = DfReading(time=rts, datafeed=df, value=float(val), restored=True)

    # now process the last cluster
    cluster = clusters[-1]
    cl_rtimestamps = list(cluster.keys())
    cl_rtimestamps.sort()

    length = len(cl_rtimestamps)
    if length > 1:
        cl_values = [cluster[rts].value for rts in cl_rtimestamps]

        spline = PchipInterpolator(cl_rtimestamps, cl_values)
        part_grid = create_grid(cl_rtimestamps[0], cl_rtimestamps[-1], time_resample)
        restored_values = spline(part_grid)

        if length >= 4:
            for rts, val in zip(part_grid, restored_values):
                if rts in cl_rtimestamps:
                    if cluster[rts].time == cl_rtimestamps[-2]:
                        # -2: restored df readings between the penultimate
                        # and the last 'native' df readings are not used
                        break
                else:
                    cluster[rts] = DfReading(time=rts, datafeed=df, value=float(val), restored=True)

    if length == 1:
        cluster[cl_rtimestamps[-1]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
    elif length == 2:
        cluster[cl_rtimestamps[-1]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
        cluster[cl_rtimestamps[-2]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
    elif length == 3:
        cluster[cl_rtimestamps[-1]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
        cluster[cl_rtimestamps[-2]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
        cluster[cl_rtimestamps[-3]].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
    else:
        cluster[cl_rtimestamps[-1]].not_to_use = NotToUseDfrTypes.SPLINE_UNCLOSED

    for cluster in clusters:
        for rts in cluster:
            if rts > start_rts:  # in order not to include those 'last_df_readings_from_prev_period'
                new_df_reading_map[rts] = cluster[rts]

    return new_df_reading_map


def restore_totalizer(
    df_reading_map: IndDfReadingMap,
    df: Datafeed,
    time_resample: int,
    time_change: int,
    start_rts: int,
    last_nat_dfr_from_prev_period,
) -> IndDfReadingMap:

    sorted_df_readings = sorted(df_reading_map.values(), key=lambda x: x.time)

    if last_nat_dfr_from_prev_period is not None:
        # maybe not the best solution from the performance point of view
        sorted_df_readings.insert(0, last_nat_dfr_from_prev_period)

    if len(sorted_df_readings) < 2:
        return df_reading_map

    new_df_reading_map = df_reading_map.copy()  # add native df readings first
    i = 0

    while i < len(sorted_df_readings):
        delta_time = sorted_df_readings[i + 1].time - sorted_df_readings[i].time
        if i == len(sorted_df_readings) - 2:  # if we reached the penultimate native df reading
            if delta_time > time_resample:
                if delta_time <= time_change:
                    sorted_df_readings[i + 1].not_to_use = NotToUseDfrTypes.SPLINE_UNCLOSED
                else:
                    sorted_df_readings[i + 1].not_to_use = NotToUseDfrTypes.SPLINE_NOT_TO_USE
            else:
                sorted_df_readings[i + 1].not_to_use = NotToUseDfrTypes.UNCLOSED
            break

        if delta_time > time_resample and delta_time <= time_change:
            grid = create_grid(sorted_df_readings[i].time, sorted_df_readings[i + 1].time, time_resample)
            k = (sorted_df_readings[i + 1].value - sorted_df_readings[i].value) / delta_time
            b = sorted_df_readings[i].value - k * sorted_df_readings[i].time
            for rts in grid:
                if rts > start_rts and rts not in new_df_reading_map:
                    new_df_reading_map[rts] = DfReading(time=rts, datafeed=df, value=k * rts + b, restored=True)
        i += 1
    return new_df_reading_map
