from collections.abc import Iterable

from django.conf import settings

from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from common.complex_types import DfValueMap


def get_end_rts(
    datafeeds: Iterable[Datafeed], time_resample: int, start_rts: int, num_df_to_process: int
) -> tuple[int, bool]:

    df_last_rtss = []

    for df in datafeeds:

        last_df_reading_rts_to_use = df.ts_to_start_with

        if last_df_reading_rts_to_use is None or last_df_reading_rts_to_use <= start_rts:
            df_last_rtss.append(start_rts)
        else:
            df_last_rtss.append(last_df_reading_rts_to_use)

    max_num_dfreadings_per_one_df_to_process = int(settings.NUM_MAX_DFREADINGS_TO_PROCESS / num_df_to_process)
    max_num_dfreadings_per_one_df_to_process = max(
        2, max_num_dfreadings_per_one_df_to_process
    )  # protection against 0 and 1
    end_rts_by_max_num_df_readings = start_rts + time_resample * max_num_dfreadings_per_one_df_to_process

    smallest_rts_in_all_last_df_readings = min(df_last_rtss)

    is_catching_up = smallest_rts_in_all_last_df_readings > end_rts_by_max_num_df_readings
    end_rts = min(end_rts_by_max_num_df_readings, smallest_rts_in_all_last_df_readings)

    return end_rts, is_catching_up


def get_df_value_map(datafeeds: Iterable[Datafeed], start_rts: int, end_rts: int) -> DfValueMap:

    df_value_map: DfValueMap = {}

    for df in datafeeds:
        df_readings = list(
            DfReading.objects.filter(datafeed__id=df.pk, time__gt=start_rts, time__lte=end_rts).order_by("time")
        )

        if len(df_readings) == 0:
            continue

        for dfr in df_readings:
            if dfr.time not in df_value_map:
                df_value_map[dfr.time] = {}
            df_value_map[dfr.time][df.name] = dfr.value

    return df_value_map
