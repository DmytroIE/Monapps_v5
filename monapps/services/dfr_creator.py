import logging
import traceback
from django.db import transaction
from django.conf import settings

from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from apps.datastreams.models import Datastream
from apps.dsreadings.models import DsReading, NoDataMarker
from apps.dfreadings.models import DfReading
from common.constants import AugmentationPolicy, DataAggrTypes, VariableTypes, NotToUseDfrTypes
from utils.ts_utils import ceil_timestamp, create_now_ts_ms

from utils.dfr_utils import (
    resample_ds_readings,
    restore_continuous_avg,
    restore_totalizer,
    resample_and_augment_ds_readings,
)
from utils.update_utils import set_attr_if_cond

logger = logging.getLogger("#dfr_creator")


class DfrCreator:
    def __init__(self, app: Application, nat_df: Datafeed) -> None:
        self.app = app
        self.df = nat_df
        self.is_catching_up = False

    @transaction.atomic
    def execute(self) -> None:

        self.df = Datafeed.objects.select_for_update().get(pk=self.df.pk)
        if self.df.datastream is None:
            raise Exception(f"Datafeed {self.df.id} has no datastream")
        self.ds = Datastream.objects.select_for_update().get(pk=self.df.datastream.pk)

        self.now_ts = create_now_ts_ms()

        is_calculated = self.calc_start_rts()
        if not is_calculated:
            self.is_catching_up = False
            return

        is_calculated = self.calc_end_rts()
        if not is_calculated:
            self.is_catching_up = False
            return

        if self.start_rts >= self.end_rts:
            # it may happen because of 'till_now_margin'
            # substraction in the 'get_end_rts' function
            self.is_catching_up = False
            return

        is_created = self.create_ds_reading_batch(settings.NUM_MAX_DSREADINGS_TO_PROCESS)
        if not is_created:
            self.is_catching_up = False
            return

        self.create_df_readings()
        self.save_df_readings()

    def calc_start_rts(self) -> bool:
        # for 'conventional' datafeeds we just use previously saved 'ts_to_start_with'
        # or 'cursor_ts' - whichever is greater
        # df readings older than 'app.cursor_ts' are not created
        self.start_rts = max(self.app.cursor_ts, self.df.ts_to_start_with)

        if self.ds.is_rbe and self.df.is_aug_on:

            last_dsr_before_start_rts = (
                DsReading.objects.filter(datastream__id=self.ds.pk, time__lte=self.start_rts).order_by("time").last()
            )

            last_ndm_before_start_rts = (
                NoDataMarker.objects.filter(datastream__id=self.ds.pk, time__lte=self.start_rts).order_by("time").last()
            )

            self.is_nd_period_open = False
            if last_ndm_before_start_rts is not None and (
                last_dsr_before_start_rts is None or last_dsr_before_start_rts.time <= last_ndm_before_start_rts.time
            ):
                self.is_nd_period_open = True

            first_dsr_after_start_rts = (
                DsReading.objects.filter(datastream__id=self.ds.pk, time__gt=self.start_rts).order_by("time").first()
            )
            first_ndm_after_start_rts = (
                NoDataMarker.objects.filter(datastream__id=self.ds.pk, time__gt=self.start_rts).order_by("time").first()
            )

            if first_dsr_after_start_rts is not None and (
                self.is_nd_period_open
                or (self.df.data_type.agg_type == DataAggrTypes.LAST and last_dsr_before_start_rts is None)
                or (
                    first_ndm_after_start_rts is not None
                    and ceil_timestamp(first_ndm_after_start_rts.time - self.df.time_resample, self.df.time_resample)
                    == self.start_rts
                )
            ):
                # shift 'start_rts' right before the first ds reading to omit 'empty' periods
                self.start_rts = ceil_timestamp(
                    first_dsr_after_start_rts.time - self.df.time_resample,
                    self.df.time_resample,
                )
        return True

    def calc_end_rts(self) -> bool:
        last_dsr = DsReading.objects.filter(datastream__id=self.ds.pk, time__gt=self.start_rts).order_by("time").last()
        if self.ds.is_rbe and self.df.is_aug_on and self.df.aug_policy == AugmentationPolicy.TILL_NOW:
            self.end_rts = ceil_timestamp(self.now_ts - self.ds.till_now_margin, self.df.time_resample)
            last_ndm = (
                NoDataMarker.objects.filter(datastream__id=self.ds.pk, time__gt=self.start_rts).order_by("time").last()
            )
            if last_ndm is not None and (last_dsr is None or last_dsr.time <= last_ndm.time):
                self.end_rts = min(self.end_rts, ceil_timestamp(last_ndm.time, self.df.time_resample))
        else:
            # for other datastreams we need at least one ds reading
            if last_dsr is None:
                # It means that there are no ds readings at all,
                # which may happen at the beginning.
                return False
            else:
                self.end_rts = ceil_timestamp(last_dsr.time, self.df.time_resample)
        return True

    def create_ds_reading_batch(self, batch_size: int) -> bool:

        # get the last ds reading in the batch
        last_dsr_in_batch = (
            DsReading.objects.filter(datastream__id=self.ds.pk, time__gt=self.start_rts).order_by("time").last()
        )

        if last_dsr_in_batch is not None:
            # add other ds readings from the last bin to the batch
            # it will improve the performance
            self.batch_end_rts = ceil_timestamp(last_dsr_in_batch.time, self.df.time_resample)
            if self.ds.is_rbe and self.df.is_aug_on:  # and self.df.aug_policy == AugmentationPolicy.TILL_NOW:
                # for 'rbe' + TILL_NOW we rather use the potential number of dfrs
                # that can be created by the aug algorithm
                potential_batch_end_rts = self.start_rts + batch_size * self.df.time_resample
                # in certain cases, 'potential_batch_end_rts' can be greater than 'self.end_rts', so crop
                self.batch_end_rts = min(potential_batch_end_rts, self.end_rts)
            self.ds_readings = list(
                DsReading.objects.filter(
                    datastream__id=self.ds.pk,
                    time__gt=self.start_rts,
                    time__lte=self.batch_end_rts,
                ).order_by("time")
            )
            return True
        else:
            if self.ds.is_rbe and self.df.is_aug_on and self.df.aug_policy == AugmentationPolicy.TILL_NOW:
                potential_batch_end_rts = self.start_rts + batch_size * self.df.time_resample
                # in certain cases, 'potential_batch_end_rts' can be greater than 'self.end_rts', so crop
                self.batch_end_rts = min(potential_batch_end_rts, self.end_rts)
                self.ds_readings = []
                return True
            else:
                return False

    def create_df_readings(self):
        var_type = self.df.data_type.var_type
        aggr_type = self.df.data_type.agg_type
        is_totalizer = self.df.data_type.is_totalizer

        if var_type == VariableTypes.CONTINUOUS and aggr_type == DataAggrTypes.AVG:
            if len(self.ds_readings) == 0:
                logger.debug("No ds readings to process")
                return
            # temperature, pressure etc
            if self.df.is_rest_on:
                if self.ds.time_change is None:
                    raise ValueError("time_change cannot be None for CONTINUOUS/AVG if restoration is on")

                # get some native df readings 'from the past' to have enough readings for spline building
                last_nat_dfrs_from_prev_period = DfReading.objects.filter(
                    datafeed__id=self.df.pk, time__lte=self.start_rts, restored=False
                ).order_by("-time")[:3]
                # Django doesn't allow negative indexes in slicing
                # that's why we use '-time' and then 'reversed'
                last_nat_dfrs_from_prev_period = list(reversed(last_nat_dfrs_from_prev_period))
            k = 1  # this number limits the number of iterations in the cylce below to avoid an infinite loop
            while True:
                self.df_reading_map = resample_ds_readings(
                    self.ds_readings, self.df, self.df.time_resample, DataAggrTypes.AVG
                )
                if self.df.is_rest_on:
                    self.df_reading_map = restore_continuous_avg(
                        self.df_reading_map,
                        self.df,
                        self.df.time_resample,
                        self.ds.time_change,
                        self.start_rts,
                        last_nat_dfrs_from_prev_period,
                    )
                    df_reading_rtss = sorted(self.df_reading_map)
                    num_spline_unclosed = 0

                    for rts in df_reading_rtss:
                        if self.df_reading_map[rts].not_to_use == NotToUseDfrTypes.SPLINE_NOT_TO_USE:
                            num_spline_unclosed += 1

                    if num_spline_unclosed == len(df_reading_rtss) and self.batch_end_rts < self.end_rts:
                        # if there are only SPLINE_NOT_TO_USE readings,
                        # and we still haven't reached the end of all the ds readings,
                        # then we can extend the batch of ds readings
                        num_dsrs_to_process = len(self.ds_readings)
                        k *= 2
                        if k > 512:
                            raise RuntimeError("DsReading batch extension limit reached")
                        num_dsrs_to_process += k
                        _ = self.create_ds_reading_batch(num_dsrs_to_process)
                    else:
                        break
                else:
                    break

        elif (
            var_type == VariableTypes.CONTINUOUS or var_type == VariableTypes.DISCRETE
        ) and aggr_type == DataAggrTypes.SUM:
            if not is_totalizer:
                if self.ds.is_rbe and self.df.is_aug_on:
                    sorted_dsrs_and_ndms = self.create_sorted_dsrs_and_ndms_list()
                    self.df_reading_map = resample_and_augment_ds_readings(
                        sorted_dsrs_and_ndms,
                        self.df,
                        self.df.time_resample,
                        self.start_rts,
                        self.batch_end_rts,
                        DataAggrTypes.SUM,
                        self.is_nd_period_open,
                    )
                else:
                    self.df_reading_map = resample_ds_readings(
                        self.ds_readings,
                        self.df,
                        self.df.time_resample,
                        DataAggrTypes.SUM,
                    )
            else:
                if self.ds.is_rbe and self.df.is_aug_on:
                    sorted_dsrs_and_ndms = self.create_sorted_dsrs_and_ndms_list()
                    dfr_at_start_ts = self.get_dfr_at_start_ts()
                    self.df_reading_map = resample_and_augment_ds_readings(
                        sorted_dsrs_and_ndms,
                        self.df,
                        self.df.time_resample,
                        self.start_rts,
                        self.batch_end_rts,
                        DataAggrTypes.LAST,
                        self.is_nd_period_open,
                        dfr_at_start_ts,
                    )
                else:
                    self.df_reading_map = resample_ds_readings(
                        self.ds_readings,
                        self.df,
                        self.df.time_resample,
                        DataAggrTypes.LAST,
                    )
                    if self.df.is_rest_on:
                        if self.ds.time_change is None:
                            raise ValueError(
                                "time_change cannot be None for CONTINUOUS/SUM+TOTALIZER if restoration is on"
                            )
                        last_nat_dfr_from_prev_period = (
                            DfReading.objects.filter(
                                datafeed__id=self.df.pk,
                                time__lte=self.start_rts,
                                restored=False,
                            )
                            .order_by("time")
                            .last()
                        )
                        self.df_reading_map = restore_totalizer(
                            self.df_reading_map,
                            self.df,
                            self.df.time_resample,
                            self.ds.time_change,
                            self.start_rts,
                            last_nat_dfr_from_prev_period,
                        )

        elif aggr_type == DataAggrTypes.LAST:  # for all var_types
            if self.ds.is_rbe and self.df.is_aug_on:
                sorted_dsrs_and_ndms = self.create_sorted_dsrs_and_ndms_list()
                dfr_at_start_ts = self.get_dfr_at_start_ts()
                self.df_reading_map = resample_and_augment_ds_readings(
                    sorted_dsrs_and_ndms,
                    self.df,
                    self.df.time_resample,
                    self.start_rts,
                    self.batch_end_rts,
                    DataAggrTypes.LAST,
                    self.is_nd_period_open,
                    dfr_at_start_ts,
                )
            else:
                self.df_reading_map = resample_ds_readings(
                    self.ds_readings,
                    self.df,
                    self.df.time_resample,
                    DataAggrTypes.LAST,
                )

        else:
            raise ValueError(
                f"""No proper resampling procedure for var type {var_type}
                            with agg type {aggr_type}"""
            )

        self.is_catching_up = self.batch_end_rts < self.end_rts

    def save_df_readings(self):

        df_readings = []
        df_reading_rtss = sorted(self.df_reading_map)

        self.rts_to_start_with_next_time = self.start_rts
        for idx, rts in enumerate(df_reading_rtss):
            if self.df_reading_map[rts].not_to_use is not None:
                if self.df_reading_map[rts].not_to_use == NotToUseDfrTypes.SPLINE_UNCLOSED:
                    if len(df_reading_rtss) == 1:
                        # 'df_reading_rtss[idx - 1]' below can give bizarre results if len == 1
                        pass
                    else:
                        self.rts_to_start_with_next_time = df_reading_rtss[idx - 1]
                else:
                    self.rts_to_start_with_next_time = rts - self.df.time_resample
                break
            df_readings.append(self.df_reading_map[rts])
            self.rts_to_start_with_next_time = rts

        last_saved_dfr_rts = None
        if len(df_readings) > 0:
            DfReading.objects.bulk_create(df_readings)
            logger.debug(f"New {len(df_readings)} df readings were saved")
            last_saved_dfr_rts = df_readings[-1].time

        set_attr_if_cond(self.rts_to_start_with_next_time, ">", self.df, "ts_to_start_with")
        if last_saved_dfr_rts is not None:
            set_attr_if_cond(last_saved_dfr_rts, ">", self.df, "last_reading_ts")
        self.df.save(update_fields=self.df.update_fields)

        set_attr_if_cond(self.rts_to_start_with_next_time, ">", self.ds, "ts_to_start_with")
        self.ds.save(update_fields=self.ds.update_fields)

    def check_catching_up(self):
        return self.is_catching_up

    def create_sorted_dsrs_and_ndms_list(self):
        nodata_markers = list(
            NoDataMarker.objects.filter(
                datastream__id=self.ds.pk,
                time__gt=self.start_rts,
                time__lte=self.batch_end_rts,
            ).order_by("time")
        )
        # as the sort provided by 'sorted' is stable,
        # then if both DsReading and NoDataMarker instances have the same timestamps,
        # then the NoDataMarker instance will be the last after the sorting
        sorted_dsrs_and_ndms = sorted(self.ds_readings + nodata_markers, key=lambda x: x.time)
        return sorted_dsrs_and_ndms

    def get_dfr_at_start_ts(self):
        return DfReading.objects.filter(datafeed__id=self.df.pk, time=self.start_rts).first()
