import logging
from scipy.interpolate import PchipInterpolator
from typing import Callable, List

from apps.datastreams.models import Datastream
from apps.datafeeds.models import Datafeed
from apps.dsreadings.models import DsReading, NoDataMarker
from apps.dfreadings.models import DfReading

from common.complex_types import IndDfReadingMap
from common.constants import DataAggrTypes, NotToUseDfrTypes, VariableTypes, AugmentationPolicy
from utils.ts_utils import ceil_timestamp, create_grid, create_now_ts_ms
from utils.alarm_utils import add_to_alarm_log

logger = logging.getLogger("#dfr_utils")


def create_df_readings(
    ds_readings: list[DsReading],
    nat_df: Datafeed,
    start_rts: int,
) -> tuple[List[DfReading], int | None, int]:
    """
    Creates datafeed readings from a set of datastream readings.
    The variable 'ds_readings' should represent readings whose timestamps
    are > 'start_rts' ('rts' means a 'rounded timestamp').
    Returns:
    'df_readings' - a list of created df readings.
    'last_dfr_rts' - a timestamp of a very last df reading created by a resample function (usually not saved).
    'rts_to_start_with_next_time' - a timestamp to start with next time.
    It is usally a timestamp before the first 'unused'/'unclosed' reading.
    """

    df_readings = []
    df_reading_map = {}
    ds = nat_df.datastream
    default_tuple = ([], None, start_rts)
    if ds is None:
        add_to_alarm_log(
            "WARNING",
            f"create_df_readings: datastream is None for datafeed {nat_df.pk}",
            instance=nat_df,
        )
        return default_tuple
    time_resample = nat_df.time_resample

    if ds.data_type.var_type == VariableTypes.CONTINUOUS and ds.data_type.agg_type == DataAggrTypes.AVG:
        # temperature, pressure etc
        if len(ds_readings) == 0:
            return default_tuple
        df_reading_map = resample_ds_readings(ds_readings, nat_df, time_resample, find_average)
        if nat_df.is_rest_on:
            if ds.time_change is None:
                raise ValueError("time_change cannot be None for CONTINUOUS/AVG if restoration is on")
            df_reading_map = restore_continuous_avg(df_reading_map, nat_df, time_resample, ds.time_change, start_rts)

    elif (
        ds.data_type.var_type == VariableTypes.CONTINUOUS or ds.data_type.var_type == VariableTypes.DISCRETE
    ) and ds.data_type.agg_type == DataAggrTypes.SUM:
        if len(ds_readings) == 0 and (not nat_df.is_aug_on or nat_df.aug_policy != AugmentationPolicy.TILL_NOW):
            return default_tuple
        if not ds.data_type.is_totalizer:
            if ds.is_rbe and nat_df.is_aug_on:
                df_reading_map = resample_and_augment_ds_readings(
                    ds_readings, nat_df, ds, time_resample, start_rts, find_sum
                )
            else:
                df_reading_map = resample_ds_readings(ds_readings, nat_df, time_resample, find_sum)
        else:
            if ds.is_rbe and nat_df.is_aug_on:
                df_reading_map = resample_and_augment_ds_readings(
                    ds_readings, nat_df, ds, time_resample, start_rts, find_last_value
                )
            else:
                df_reading_map = resample_ds_readings(ds_readings, nat_df, time_resample, find_last_value)
                if nat_df.is_rest_on:
                    if ds.time_change is None:
                        raise ValueError("time_change cannot be None for CONTINUOUS/AVG if restoration is on")
                    df_reading_map = restore_totalizer(df_reading_map, nat_df, time_resample, ds.time_change, start_rts)

    elif ds.data_type.agg_type == DataAggrTypes.LAST:  # for all var_types
        if len(ds_readings) == 0 and (not nat_df.is_aug_on or nat_df.aug_policy != AugmentationPolicy.TILL_NOW):
            return default_tuple
        if ds.is_rbe and nat_df.is_aug_on:
            df_reading_map = resample_and_augment_ds_readings(
                ds_readings, nat_df, ds, time_resample, start_rts, find_last_value
            )
        else:
            df_reading_map = resample_ds_readings(ds_readings, nat_df, time_resample, find_last_value)

    else:
        raise ValueError(
            f"""No proper resampling procedure for var type {ds.data_type.var_type}
                         with agg type {ds.data_type.agg_type}"""
        )

    # now it is necessary to save in the database all the df readings except 'not_to_use' readings
    df_reading_rtss = sorted(df_reading_map)

    last_dfr_rts = None
    rts_to_start_with_next_time = start_rts

    for idx, rts in enumerate(df_reading_rtss):
        if df_reading_map[rts].not_to_use is not None:
            if df_reading_map[rts].not_to_use == NotToUseDfrTypes.SPLINE_UNCLOSED:
                if len(df_reading_rtss) == 1:
                    # 'df_reading_rtss[idx - 1]' below can give bizarre results if len == 1
                    pass
                else:
                    rts_to_start_with_next_time = df_reading_rtss[idx - 1]
            else:
                rts_to_start_with_next_time = rts - time_resample
            break
        df_readings.append(df_reading_map[rts])
        rts_to_start_with_next_time = rts

    if len(df_reading_rtss) > 0:  # almost impossible that len(df_reading_rtss) == 0 if we got to this point
        last_dfr_rts = max(df_reading_rtss)  # it is the ts of the last (unclosed) df reading

    return df_readings, last_dfr_rts, rts_to_start_with_next_time


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


def resample_ds_readings(
    ds_readings: list[DsReading], df: Datafeed, time_resample: int, agg_func: Callable[[list[DsReading]], float | None]
) -> IndDfReadingMap:
    """
    A generic function, can be used with different aggregation functions.
    """

    sorted_ds_readings = sorted(ds_readings, key=lambda x: x.time)
    df_reading_map = {}

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
    ds_readings: list[DsReading],
    df: Datafeed,
    ds: Datastream,
    time_resample: int,
    start_rts: int,
    agg_func: Callable[[list[DsReading]], float | int | None],
) -> IndDfReadingMap:
    """
    Assumes that 'df.is_aug_on' is True
    Timestamps of the instances in 'ds_readings' should be > 'start_rts'
    """

    if len(ds_readings) == 0 and df.aug_policy != AugmentationPolicy.TILL_NOW:
        return {}

    df_reading_map = {}

    nodata_markers = list(NoDataMarker.objects.filter(datastream__id=ds.pk, time__gt=start_rts).order_by("time"))

    # as the sort provided by 'sorted' is stable,
    # then if both DsReading and NoDataMarker instances have the same timestamps,
    # then the NoDataMarker instance will be the last after the sorting
    sorted_ds_readings_and_nodata_markers = sorted(ds_readings + nodata_markers, key=lambda x: x.time)

    last_df_reading_rts = None  # it may happen that there are no ds readings in 'ds_readings'
    for r in sorted_ds_readings_and_nodata_markers:
        rts = ceil_timestamp(r.time, time_resample)
        if rts not in df_reading_map:
            df_reading_map[rts] = []

        df_reading_map[rts].append(r)
        if isinstance(r, DsReading):
            last_df_reading_rts = rts

    last_dfr_from_prev_period = DfReading.objects.filter(datafeed__id=df.pk, time=start_rts).order_by("time").first()
    # add this dfr temporarily, it will be removed at the end of the function
    if last_dfr_from_prev_period is not None:
        df_reading_map[start_rts] = last_dfr_from_prev_period

    else:  # for SUM + TILL_NOW it is necessary to check if there is a NoDataMarker at the last position
        if ds.data_type.agg_type == DataAggrTypes.SUM and df.aug_policy == AugmentationPolicy.TILL_NOW:
            last_dsr_before_start_rts = (
                DsReading.objects.filter(datastream__id=ds.pk, time__lte=start_rts).order_by("time").last()
            )
            last_ndm_before_start_rts = (
                NoDataMarker.objects.filter(datastream__id=ds.pk, time__lte=start_rts).order_by("time").last()
            )
            if last_ndm_before_start_rts is None or (
                last_dsr_before_start_rts is not None
                and last_dsr_before_start_rts.time > last_ndm_before_start_rts.time
            ):
                # temporarily add this point
                df_reading_map[start_rts] = DfReading(time=start_rts, value=0, datafeed=df, restored=True)

    # create a grid according to the augmentation policy
    if df.aug_policy == AugmentationPolicy.TILL_LAST_DF_READING:
        if last_df_reading_rts is None:  # impossible to have it None at this point, just for the linter
            return {}
        end_rts_acc_to_aug_policy = last_df_reading_rts
    elif df.aug_policy == AugmentationPolicy.TILL_NOW:
        end_rts_acc_to_aug_policy = ceil_timestamp(create_now_ts_ms() - ds.till_now_margin, time_resample)
    else:
        raise ValueError("Wrong augmentation policy")

    grid = create_grid(start_rts + time_resample, end_rts_acc_to_aug_policy, time_resample)

    last_df_reading_rts = None
    is_nodata_period = df_reading_map.get(start_rts, None) is None

    for rts in grid:
        arr = df_reading_map.get(rts, None)  # 'arr' is already sorted by time
        if arr is not None:
            new_arr = [r for r in arr if isinstance(r, DsReading)]
            agg_value = agg_func(new_arr)  # if 'new_arr' is empty, 'agg_func' will return None

            if agg_value is not None:
                is_nodata_period = False
                dfr = DfReading(time=rts, value=agg_value, datafeed=df, restored=False)
                df_reading_map[rts] = dfr
                last_df_reading_rts = rts
            else:  # only nodata marker in 'arr'
                del df_reading_map[rts]

            if isinstance(arr[-1], NoDataMarker):  # if the last item in 'arr' is NoDataMarker
                is_nodata_period = True
        else:
            if not is_nodata_period:
                prev_dfr = df_reading_map.get(rts - time_resample, None)
                if prev_dfr is not None:
                    if ds.data_type.agg_type == DataAggrTypes.SUM:
                        dfr = DfReading(time=rts, value=0, datafeed=df, restored=True)
                    elif ds.data_type.agg_type == DataAggrTypes.LAST:
                        dfr = DfReading(time=rts, value=prev_dfr.value, datafeed=df, restored=True)
                    else:
                        raise ValueError(f"Unknown augmentation type for {ds.data_type.agg_type}")
                    df_reading_map[rts] = dfr
                    last_df_reading_rts = rts

    # removing the dfr taken from the previous period
    if df_reading_map.get(start_rts, None) is not None:
        del df_reading_map[start_rts]

    # injection of 'not_to_use' property
    if last_df_reading_rts is not None:
        # if not df_reading_map[last_df_reading_rts].restored:
        df_reading_map[last_df_reading_rts].not_to_use = NotToUseDfrTypes.UNCLOSED

    return df_reading_map


# For 'continuous + AVG' datastreams
def restore_continuous_avg(
    df_reading_map: IndDfReadingMap, df: Datafeed, time_resample: int, time_change: int, start_rts: int
) -> IndDfReadingMap:

    sorted_df_readings = sorted(df_reading_map.values(), key=lambda x: x.time)

    for dfr in sorted_df_readings:  # NOTE: just in case, probably not necessary at all
        dfr.not_to_use = None

    # get some readings 'from the past' to have enough readings for interpolation
    last_df_readings_from_prev_period = DfReading.objects.filter(
        datafeed__id=df.pk, time__lte=start_rts, restored=False
    ).order_by("-time")[:3]  # Django doesn't allow negative indexes in slicing
    # that's why we use '-time' and then 'reversed'
    last_df_readings_from_prev_period = list(reversed(last_df_readings_from_prev_period))

    # add some readings 'from the past' to have enough readings for interpolation
    next_rts = sorted_df_readings[0].time
    i = len(last_df_readings_from_prev_period) - 1
    if i >= 0:
        while i >= 0:
            if next_rts - last_df_readings_from_prev_period[i].time <= time_change:
                # TODO: prepending operation,
                # maybe not the best solution from the performance point of view
                sorted_df_readings.insert(0, last_df_readings_from_prev_period[i])
            else:
                break
            next_rts = last_df_readings_from_prev_period[i].time
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
    # i.e. groups of DfReadings, the distance between groups is >= 't_change'
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
    df_reading_map: IndDfReadingMap, df: Datafeed, time_resample: int, time_change: int, start_rts: int
) -> IndDfReadingMap:

    sorted_df_readings = sorted(df_reading_map.values(), key=lambda x: x.time)

    last_dfr_from_prev_period = DfReading.objects.filter(
        datafeed__id=df.pk, time__lte=start_rts, restored=False
    ).first()

    if last_dfr_from_prev_period is not None:
        # maybe not the best solution from the performance point of view
        sorted_df_readings.insert(0, last_dfr_from_prev_period)

    if len(sorted_df_readings) < 2:
        return df_reading_map

    sorted_df_readings[-1].not_to_use = NotToUseDfrTypes.SPLINE_UNCLOSED

    new_df_reading_map = df_reading_map.copy()
    i = 1
    while i < len(sorted_df_readings):
        if i - 1 == len(sorted_df_readings) - 2:
            # -2: restored df readings between the penultimate
            # and the last 'native' df readings are not used
            break
        delta_time = sorted_df_readings[i].time - sorted_df_readings[i - 1].time
        if delta_time > time_resample and delta_time <= time_change:
            grid = create_grid(sorted_df_readings[i - 1].time, sorted_df_readings[i].time, time_resample)
            k = (sorted_df_readings[i].value - sorted_df_readings[i - 1].value) / delta_time
            b = sorted_df_readings[i - 1].value - k * sorted_df_readings[i - 1].time
            for rts in grid:
                if rts > start_rts and rts not in new_df_reading_map:
                    new_df_reading_map[rts] = DfReading(time=rts, datafeed=df, value=k * rts + b, restored=True)
        i += 1
    return new_df_reading_map
