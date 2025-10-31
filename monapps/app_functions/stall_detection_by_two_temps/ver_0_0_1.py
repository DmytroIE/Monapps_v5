import logging
from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from common.constants import CurrStateTypes, HealthGrades
from utils.ts_utils import create_grid
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from common.constants import CURR_STATE_FIELD_NAME
from utils.app_func_utils import get_end_rts, get_df_value_map
from utils.alarm_utils import add_to_alarm_payload

logger = logging.getLogger("#stall_det_0_0_1")


def function(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:
    """
    It's a legacy function that is left just for demo purposes.
    Don't use it in production.
    """

    temp_in_df = native_df_map["Temp inlet"]
    temp_out_df = native_df_map["Temp outlet"]
    curr_state_df = derived_df_map[CURR_STATE_FIELD_NAME]

    start_rts = app.cursor_ts
    num_df_to_process = 3  # 3 is because we use 2 temperature datafeed + 1 curr_state datafeed
    end_rts, is_catching_up = get_end_rts(native_df_map.values(), app.time_resample, start_rts, num_df_to_process)

    update_map: UpdateMap = {}
    alarm_payload = {}  # {1734567890123: {"e": {"Wrong data":{}, "Something else": {"st": "in"}}, "w": {...}}, ...}

    derived_df_reading_map: DerivedDfReadingMap = {CURR_STATE_FIELD_NAME: {"df": curr_state_df, "new_df_readings": []}}

    if end_rts > start_rts:  # all datafeed have readings with ts > cursor_ts

        df_value_map = get_df_value_map(native_df_map.values(), start_rts, end_rts)

        grid = create_grid(start_rts + app.time_resample, end_rts, app.time_resample)

        delta_t_in = app.settings.get("delta_t_in", 10)
        delta_t_out = app.settings.get("delta_t_out", 5)

        prev_curr_state_dfr = DfReading.objects.filter(datafeed__id=curr_state_df.pk, time=start_rts).first()

        if prev_curr_state_dfr is None:
            prev_curr_state = CurrStateTypes.UNDEFINED
        else:
            prev_curr_state = prev_curr_state_dfr.value

        curr_state = CurrStateTypes.UNDEFINED
        rts = None

        for rts in grid:
            alarm_payload[rts] = {}  # NOTE: this is very important
            curr_state = CurrStateTypes.UNDEFINED

            line = df_value_map.get(rts, None)
            temp_inlet = None
            temp_outlet = None
            if line is not None:
                temp_inlet = line.get(temp_in_df.name, None)
                temp_outlet = line.get(temp_out_df.name, None)

            if temp_inlet is not None and temp_outlet is not None:
                if temp_outlet - temp_inlet > 0.5:
                    update_map["health"] = HealthGrades.ERROR
                    add_to_alarm_payload(alarm_payload, "Temp outlet > Temp inlet", {}, rts, "w")
                else:
                    if temp_inlet - temp_outlet > delta_t_in:
                        curr_state = CurrStateTypes.WARNING
                    elif temp_inlet - temp_outlet < delta_t_in and prev_curr_state < CurrStateTypes.WARNING:
                        # for the cases when we start the app with delta T
                        # between 'delta_t_in' and 'delta_t_out'
                        curr_state = CurrStateTypes.OK
                    elif temp_inlet - temp_outlet < delta_t_out:
                        curr_state = CurrStateTypes.OK
                    else:
                        curr_state = prev_curr_state

            if curr_state != prev_curr_state:
                if curr_state == CurrStateTypes.WARNING:
                    add_to_alarm_payload(alarm_payload, "Stall detected", {"st": "in"}, rts, "w")
                else:
                    add_to_alarm_payload(alarm_payload, "Stall detected", {"st": "out"}, rts, "w")

            prev_curr_state = curr_state

            dfr = DfReading(time=rts, value=curr_state, datafeed=curr_state_df, restored=False)
            derived_df_reading_map[CURR_STATE_FIELD_NAME]["new_df_readings"].append(dfr)

        update_map["cursor_ts"] = end_rts
        update_map["is_catching_up"] = is_catching_up
        update_map["alarm_payload"] = alarm_payload

    return derived_df_reading_map, update_map


df_schema = {
    "Temp input": {"derived": False, "data_type": "Temperature"},
    "Temp output": {"derived": False, "data_type": "Temperature"},
    CURR_STATE_FIELD_NAME: {"derived": True, "data_type": CURR_STATE_FIELD_NAME},
}

settings_jsonschema = {
    "delta_t_in": {"type": "number", "maximum": 10, "minimum": 0},
    "t_delay_ms": {"type": "integer", "maximum": 300000, "minimum": 0},
    "delta_t_out": {"type": "number", "maximum": 10, "minimum": 0},
}

settings_jsonschema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "#/settings/stall_detection_by_two_temps_0_0_1",
    "title": "App settings",
    "description": "Settings for <Stall detection by two temps 0.0.1>",
    "type": "object",
    "properties": {
        "delta_t_in": {"type": "number", "maximum": 50, "minimum": 1},
        "t_delay_ms": {"type": "number", "maximum": 300000, "minimum": 0},
        "delta_t_out": {"type": "number", "maximum": 50, "minimum": 1},
    },
}

stall_detection_by_two_temps_0_0_1 = {
    "function": function,
    "df_schema": df_schema,
    "settings_jsonschema": settings_jsonschema,
    "aux_jsonschemas": {},
}
