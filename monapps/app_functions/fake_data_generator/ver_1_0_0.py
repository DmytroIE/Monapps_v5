import logging
import random
import time
from django.conf import settings
from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from utils.ts_utils import floor_timestamp, create_now_ts_ms
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from common.constants import STATUS_FIELD_NAME, CURR_STATE_FIELD_NAME

logger = logging.getLogger("#fake_data_gen_1_0_0")


def function(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:
    """
    Used as a generator of different status and current state values for testing the update algorithms.
    Also, sometimes can generate exceptions to test the exception handling in the wrapper"
    """

    status_df = derived_df_map[STATUS_FIELD_NAME]
    curr_state_df = derived_df_map[CURR_STATE_FIELD_NAME]

    update_map: UpdateMap = {}
    derived_df_reading_map: DerivedDfReadingMap = {
        STATUS_FIELD_NAME: {"df": status_df, "new_df_readings": []},
        CURR_STATE_FIELD_NAME: {"df": curr_state_df, "new_df_readings": []},
    }

    end_rts = floor_timestamp(create_now_ts_ms(), app.time_resample)
    if end_rts == app.cursor_ts:
        # it means that the function is invoked too often, more often than time_resample
        # it is necessary to skip the invocation, otherwise new df_readings with already
        # existing ts will be created, which will lead to an integrity error
        logger.debug("Function is invoked too often, skipping...")
        return derived_df_reading_map, update_map

    prob_exeption = app.settings.get("prob_exeption", 0.3)
    prob_calc_omitted = app.settings.get("prob_calc_omitted", 0.3)
    prob_error = app.settings.get("prob_error", 0.3)
    prob_warning = app.settings.get("prob_warning", 0.3)

    # imitate synchronous delay
    time.sleep(random.randrange(1, 4))

    # sometimes generate an exception to check 'excep_health'
    var = None
    if random.random() < prob_exeption:
        var = 1 / 0  # generate an exception

    num_df_to_process = 2  # status_df + curr_state_df
    max_num_dfreadings_per_one_df_to_process = int(settings.NUM_MAX_DFREADINGS_TO_PROCESS / num_df_to_process)
    max_num_dfreadings_per_one_df_to_process = max(
        2, max_num_dfreadings_per_one_df_to_process
    )  # protection against 0 and 1
    end_rts_by_max_num_df_readings = app.cursor_ts + app.time_resample * max_num_dfreadings_per_one_df_to_process
    is_catching_up = end_rts > end_rts_by_max_num_df_readings
    update_map["is_catching_up"] = is_catching_up
    end_rts = min(end_rts, end_rts_by_max_num_df_readings)

    alarm_payload = {}

    rts = app.cursor_ts + app.time_resample
    while rts <= end_rts:
        if random.random() > prob_calc_omitted:
            curr_state = random.randint(1, 3)
            status = random.randint(1, 3)
        else:
            curr_state = 0
            status = 0

        dfr = DfReading(time=rts, value=curr_state, datafeed=curr_state_df, restored=False)
        derived_df_reading_map[CURR_STATE_FIELD_NAME]["new_df_readings"].append(dfr)
        dfr = DfReading(time=rts, value=status, datafeed=status_df, restored=False)
        derived_df_reading_map[STATUS_FIELD_NAME]["new_df_readings"].append(dfr)

        alarm_payload[rts] = {}

        if random.random() < prob_error:
            alarm_payload[rts]["e"] = {"Error": {}}

        if random.random() < prob_warning:
            alarm_payload[rts]["w"] = {"Warning": {}}

        rts += app.time_resample

    update_map["cursor_ts"] = end_rts
    update_map["alarm_payload"] = alarm_payload

    print("\n")
    print(update_map)
    print("\n")

    return derived_df_reading_map, update_map


df_schema = {
    STATUS_FIELD_NAME: {"derived": True, "data_type": STATUS_FIELD_NAME},
    CURR_STATE_FIELD_NAME: {"derived": True, "data_type": CURR_STATE_FIELD_NAME},
}


settings_jsonschema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "#/settings/fake_data_generator_1_0_0",
    "title": "App settings",
    "description": "Settings for <Fake data genetartor 1.0.0>",
    "type": "object",
    "properties": {
        "prob_exeption": {"type": "number", "maximum": 1, "minimum": 0},
        "prob_calc_omitted": {"type": "number", "maximum": 1, "minimum": 0},
        "prob_error": {"type": "number", "maximum": 1, "minimum": 0},
        "prob_warning": {"type": "number", "maximum": 1, "minimum": 0},
    },
}


fake_data_generator_1_0_0 = {
    "function": function,
    "df_schema": df_schema,
    "settings_jsonschema": settings_jsonschema,
    "aux_jsonschemas": {},
}
