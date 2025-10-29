import logging
import traceback
from django.db import transaction
from django.conf import settings

from apps.applications.models import Application
from apps.datastreams.models import Datastream
from apps.datafeeds.models import Datafeed
from apps.dsreadings.models import DsReading
from apps.dfreadings.models import DfReading
from common.constants import AugmentationPolicy
from utils.dfr_utils import create_df_readings
from utils.ts_utils import ceil_timestamp


logger = logging.getLogger("#dfr_creator")


class NewDfrCreator:
    def __init__(self, app: Application):
        self.app = app

    def execute(self):
        if not self.app.is_enabled:
            return

        native_df_qs = self.app.get_native_df_qs()
        for df in native_df_qs:
            try:
                self.create_df_readings_for_ind_df(df)
            except Exception:
                logger.error(f"Error while creating readings for df {df.pk} {df.name}, {traceback.format_exc(-1)}")

    @transaction.atomic
    def create_df_readings_for_ind_df(self, nat_df: Datafeed) -> None:
        ds = nat_df.datastream
        if ds is None:  # shouldn't be possible at all, just in case
            return

        # lock both df and ds
        nat_df = Datafeed.objects.select_for_update().get(pk=nat_df.pk)
        ds = Datastream.objects.select_for_update().get(pk=ds.pk)

        # first, find the rts up to which new df readings will be created (the rts itself is included)
        last_dsr = DsReading.objects.filter(datastream__id=ds.pk).order_by("time").last()
        if last_dsr is None:
            # it means that there are no ds readings at all,
            # it may happen at the beginning of evaluation
            if nat_df.is_aug_on and nat_df.aug_policy == AugmentationPolicy.TILL_NOW:
                end_rts_by_very_last_ds_reading = 0  # it can be any small value, 0 works well here
            else:
                return
        else:
            end_rts_by_very_last_ds_reading = ceil_timestamp(last_dsr.time, self.app.time_resample)

        # then find the rts starting from which new df readings will be created (the rts itself is not included)
        # df readings older than 'app.cursor_ts' are not created
        start_rts = max(self.app.cursor_ts, nat_df.ts_to_start_with)

        last_saved_dfr_rts = None

        num_dsrs_to_process = settings.NUM_MAX_DSREADINGS_TO_PROCESS
        # if there are too many ds readings, they are processed in batches of size 'NUM_MAX_DSREADINGS_TO_PROCESS'
        while True:
            ds_readings = list(
                DsReading.objects.filter(datastream__id=ds.pk, time__gt=start_rts).order_by("time")[
                    :num_dsrs_to_process
                ]
            )

            df_readings, last_dfr_rts, rts_to_start_with_next_time = create_df_readings(
                ds_readings, nat_df, start_rts
            )

            last_saved_dfr_rts = None
            if len(df_readings) > 0:
                logger.debug(f"Saving {len(df_readings)} readings for df {nat_df.pk} '{nat_df.name}'")
                DfReading.objects.bulk_create(df_readings)
                last_saved_dfr_rts = df_readings[-1].time
                logger.debug(f"Last saved dfr rts: {last_saved_dfr_rts}")

            if (
                # no new df readins were created
                last_dfr_rts is None
                # or all new ds readings have been processed
                or last_dfr_rts >= end_rts_by_very_last_ds_reading
            ):
                # all possible ds readings were processed
                # '>=', not '==', because for TILL_NOW there can be
                # ds readings with rts > than the last dsr ts
                break
            elif last_dfr_rts is not None and last_saved_dfr_rts is None:
                # it may happen that because of the limitation for max ds readings to process
                # (when 'NUM_MAX_DSREADINGS_TO_PROCESS' is small)
                # only 1-3 unclosed df readings were created from ds readings and therefore these df readings
                # were not saved inside the 'prep_df_readings' function.
                # 'last_saved_dfr_rts' then will be None.
                # But if we got to this point (which means that the condition
                # 'last_dfr_rts' >= 'end_rts_by_very_last_ds_reading'
                # was not true), it means that there are more ds readings ahead of 'last_dfr_rts'.
                # In this case, we need to increase the number of ds readings processed in one subcycle
                # in order not to let the processing cycle run on the spot.
                # This is an extremely far-fetched situation (when, sat, MAX_NUM is measured in tens or so),
                # but for consistency it is necessary to include this handler into the code.
                num_dsrs_to_process += 1
            elif rts_to_start_with_next_time == start_rts:  # no readings were processed, maybe an unnecessary branch
                break
            else:
                num_dsrs_to_process = settings.NUM_MAX_DSREADINGS_TO_PROCESS
                start_rts = rts_to_start_with_next_time

        if rts_to_start_with_next_time > nat_df.ts_to_start_with:
            nat_df.ts_to_start_with = rts_to_start_with_next_time
            nat_df.update_fields.add("ts_to_start_with")
        if last_saved_dfr_rts is not None:
            nat_df.last_reading_ts = last_saved_dfr_rts
            nat_df.update_fields.add("last_reading_ts")
        nat_df.save(update_fields=nat_df.update_fields)

        if rts_to_start_with_next_time > ds.ts_to_start_with:
            ds.ts_to_start_with = rts_to_start_with_next_time
            ds.update_fields.add("ts_to_start_with")
        ds.save(update_fields=ds.update_fields)

        # 'rts_to_start_with_next_time' is used for several purposes:
        # 1. as the last rts that can be used by the app function
        # 2. as an rts to start with when bringing ds readings from the db and creating new df readings
        # Ideally, there should be another variable for purpose 2, which would have in most cases
        # a bit bigger value than the variable for purpose 1
        # (because in the most cases it would be an rts before the
        # last df reading produced by a resampling function).
        # But for the sake of simplicity only one variable is used.
