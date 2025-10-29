import logging
from functools import partial
import copy
from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from apps.dfreadings.models import DfReading
from utils.ts_utils import create_grid
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from common.constants import STATUS_FIELD_NAME, CURR_STATE_FIELD_NAME
from utils.app_func_utils import get_end_rts, get_df_value_map
from utils.alarm_utils import add_to_alarm_payload

from app_functions.helpers.automatas.automata_conditions import ConditionType1, InitDictForConditionType1
from app_functions.helpers.utils.occ_cluster_list import OccurrenceClusterList
from app_functions.helpers.automatas.curr_state_automata_type1 import CurrStateAutomataType1
from app_functions.helpers.automatas.status_automata_type1 import StatusAutomataType1

logger = logging.getLogger("#stall_det_1_0_0")


TEMP_DIFF_ERROR_THRESHOLD = 0.1


def function(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:

    logger.info("'stall_detection_by_two_temps_1_0_0' starts executing...")

    # get datafeeds
    temp_in_df = native_df_map["Temp in"]
    temp_out_df = native_df_map["Temp out"]
    status_df = derived_df_map[STATUS_FIELD_NAME]
    curr_state_df = derived_df_map[CURR_STATE_FIELD_NAME]

    # prepare other variables
    update_map: UpdateMap = {}
    alarm_payload = {}  # {1734567890123: {"e": {"Wrong data":{}, "Something else": {"st": "in"}}, "w": {...}}, ...}

    derived_df_reading_map: DerivedDfReadingMap = {
        STATUS_FIELD_NAME: {"df": status_df, "new_df_readings": []},
        CURR_STATE_FIELD_NAME: {"df": curr_state_df, "new_df_readings": []},
    }

    # get end time
    start_rts = app.cursor_ts
    num_df_to_process = 4  # 4 is because we use 2 temperature datafeeds + curr_state datafeed + status datafeed
    end_rts, is_catching_up = get_end_rts(native_df_map.values(), app.time_resample, start_rts, num_df_to_process)

    if end_rts > start_rts:  # all datafeed have readings with ts > cursor_ts

        # -1- get app settings
        # -1-1- get app settings for curr_state automata
        delta_temp = app.settings.get("delta_temp", 10.0)
        temp_in_threshold = app.settings.get("temp_in_threshold", 50.0)
        # current state transition counts - how many counts in a row needed to fulfill the condition
        cs_trans_counts = app.settings.get("cs_trans_counts", 3)

        # -1-2- get app settings for status automata
        default_undef_cond_dict: InitDictForConditionType1 = {
            "total_occs": 30 * 24 * 60,
            "ok_cond": "==",
            "num_of_ok_occs": 0,
            "warn_cond": "==",
            "num_of_warn_occs": 0,
            "undef_cond": ">=",
            "num_of_undef_occs": 30 * 24 * 60,
        }
        undef_cond = ConditionType1(app.settings.get("undef_cond", default_undef_cond_dict))

        default_ok_from_warn_cond_dict: InitDictForConditionType1 = {
            "total_occs": 30 * 24 * 60,
            "num_of_undef_occs": 0,
            "undef_cond": ">=",
            "num_of_ok_occs": 15 * 24 * 60,
            "ok_cond": ">=",
            "num_of_warn_occs": 0,
            "warn_cond": "==",
        }
        ok_from_warn_cond = ConditionType1(app.settings.get("ok_from_warn_cond", default_ok_from_warn_cond_dict))

        default_warn_cond_dict: InitDictForConditionType1 = {
            "total_occs": 5 * 24 * 60,
            "ok_cond": ">=",
            "num_of_ok_occs": 0,
            "warn_cond": ">=",
            "num_of_warn_occs": 1 * 24 * 60,
            "undef_cond": ">=",
            "num_of_undef_occs": 0,
        }
        warn_cond = ConditionType1(app.settings.get("warn_cond", default_warn_cond_dict))

        default_ok_from_undef_cond_dict: InitDictForConditionType1 = {
            "total_occs": 1 * 24 * 60,
            "num_of_undef_occs": 0,
            "undef_cond": ">=",
            "num_of_ok_occs": 12 * 60,
            "ok_cond": ">=",
            "num_of_warn_occs": 0,
            "warn_cond": "==",
        }
        ok_from_undef_cond = ConditionType1(app.settings.get("ok_from_undef_cond", default_ok_from_undef_cond_dict))

        # -2- get app state

        # -2-1- get app state for curr_state automata
        cs_automata_state = CurrStateAutomataType1.States(
            app.state.get("cs_automata_state", CurrStateAutomataType1.States.UNDEFINED.value)
        )  # to start with UNDEFINED
        cs_automata_prev_state = CurrStateAutomataType1.States(
            app.state.get("cs_automata_prev_state", CurrStateAutomataType1.States.OFF.value)
        )  # any value different from UNDEFINED
        err_counts = app.state.get("err_counts", 0)
        off_counts = app.state.get("off_counts", 0)
        ok_counts = app.state.get("ok_counts", 0)
        warn_counts = app.state.get("warn_counts", 0)

        # -2-2- get app state for status automata
        st_automata_state = StatusAutomataType1.States(
            app.state.get("st_automata_state", StatusAutomataType1.States.UNDEFINED.value)
        )
        st_automata_prev_state = StatusAutomataType1.States(
            app.state.get("st_automata_prev_state", StatusAutomataType1.States.OK.value)
        )

        all_occs = OccurrenceClusterList(app.state.get("all_occs"))

        # -3- create automatas

        add_to_alarm_payload_part = partial(add_to_alarm_payload, alarm_payload)
        # create an automata instance for "current state"
        cs_automata = CurrStateAutomataType1(
            cs_automata_state,
            cs_automata_prev_state,
            add_to_alarm_payload_part,
            cs_trans_counts,
            err_counts=err_counts,
            off_counts=off_counts,
            ok_counts=ok_counts,
            warn_counts=warn_counts,
        )

        # create an automata instance for "status"
        st_automata = StatusAutomataType1(
            st_automata_state,
            st_automata_prev_state,
            add_to_alarm_payload_part,
            undef_cond,
            ok_from_undef_cond,
            ok_from_warn_cond,
            warn_cond,
        )

        # -4- get new df values as a map
        df_value_map = get_df_value_map(native_df_map.values(), start_rts, end_rts)

        # -5- create grid
        grid = create_grid(start_rts + app.time_resample, end_rts, app.time_resample)

        # -6- moving along the grid
        for rts in grid:
            alarm_payload[rts] = {}  # NOTE: this is very important, add at least an empty dict for each rts

            # -6-1- evaluate current state
            line = df_value_map.get(rts, None)
            temp_in = None
            temp_out = None
            if line is not None:
                temp_in = line.get(temp_in_df.name, None)
                temp_out = line.get(temp_out_df.name, None)

            cs_err_flag = temp_in is None or temp_out is None or temp_out - temp_in > TEMP_DIFF_ERROR_THRESHOLD
            cs_off_flag = not cs_err_flag and temp_in <= temp_in_threshold
            cs_ok_flag = not cs_err_flag and temp_in - temp_out <= delta_temp
            cs_warn_flag = not cs_err_flag and temp_in - temp_out > delta_temp

            # execute CS finite automata
            cs_automata.execute(rts, cs_err_flag, cs_off_flag, cs_ok_flag, cs_warn_flag)

            # get the results
            curr_state = cs_automata.curr_state

            cs_dfr = DfReading(time=rts, value=curr_state, datafeed=curr_state_df, restored=False)
            derived_df_reading_map[CURR_STATE_FIELD_NAME]["new_df_readings"].append(cs_dfr)

            logger.debug(f"Curr State Automata state = {cs_automata.state}")

            # -6-2- update interval maps
            all_occs.append_occurrence(curr_state.value)  # NOTE: 'value' to serialize JSON

            # -6-3- evaluate status

            # execute ST finite automata
            st_automata.execute(rts, all_occs)

            # get the results
            status = st_automata.status

            st_dfr = DfReading(time=rts, value=status, datafeed=status_df, restored=False)
            derived_df_reading_map[STATUS_FIELD_NAME]["new_df_readings"].append(st_dfr)

            logger.debug(f"Status Automata state = {st_automata.state}")

        # -7-  update app output
        update_map["cursor_ts"] = end_rts
        update_map["is_catching_up"] = is_catching_up
        update_map["alarm_payload"] = alarm_payload
        update_map["health"] = cs_automata.health_from_app

        updated_state = copy.deepcopy(app.state)
        updated_state["cs_automata_state"] = cs_automata.state.value  # NOTE: 'value' to serialize JSON
        updated_state["cs_automata_prev_state"] = cs_automata.prev_state.value  # NOTE: 'value' to serialize JSON
        updated_state["err_counts"] = cs_automata.err_counter.counts
        updated_state["off_counts"] = cs_automata.off_counter.counts
        updated_state["warn_counts"] = cs_automata.warn_counter.counts
        updated_state["ok_counts"] = cs_automata.ok_counter.counts
        updated_state["st_automata_state"] = st_automata.state.value  # NOTE: 'value' to serialize JSON
        updated_state["st_automata_prev_state"] = st_automata.prev_state.value  # NOTE: 'value' to serialize JSON
        updated_state["all_occs"] = all_occs
        update_map["state"] = updated_state

    return derived_df_reading_map, update_map


df_schema = {
    "Temp in": {"derived": False, "data_type": "Temperature"},
    "Temp out": {"derived": False, "data_type": "Temperature"},
    CURR_STATE_FIELD_NAME: {"derived": True, "data_type": CURR_STATE_FIELD_NAME},
    STATUS_FIELD_NAME: {"derived": True, "data_type": STATUS_FIELD_NAME},
}

condition_jsonschema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "#/conditions/type1",
    "title": "Condition type 1",
    "description": "Conditions for state transitions of StatusAutomataType1",
    "type": "object",
    "properties": {
        "total_occs": {"type": "number", "maximum": 100000, "minimum": 1},
        "ok_cond": {"type": "string", "enum": ["==", ">=", "<="]},
        "num_of_ok_occs": {"type": "number", "maximum": 100000, "minimum": 0},
        "warn_cond": {"type": "string", "enum": ["==", ">=", "<="]},
        "num_of_warn_occs": {"type": "number", "maximum": 100000, "minimum": 0},
        "undef_cond": {"type": "string", "enum": ["==", ">=", "<="]},
        "num_of_undef_occs": {"type": "number", "maximum": 100000, "minimum": 0},
    },
}

settings_jsonschema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "#/settings/stall_detection_by_two_temps_1_0_0",
    "title": "App settings",
    "description": "Settings for <Stall detection by two temps 1.0.0>",
    "type": "object",
    "properties": {
        "delta_temp": {"type": "number", "maximum": 60, "minimum": 1},
        "temp_in_threshold": {"type": "number", "maximum": 150, "minimum": 10},
        "cs_trans_counts": {"type": "number", "maximum": 10, "minimum": 1},
        "undef_cond": {"$ref": "#/conditions/type1"},
        "ok_from_warn_cond": {"$ref": "#/conditions/type1"},
        "warn_cond": {"$ref": "#/conditions/type1"},
        "ok_from_undef_cond": {"$ref": "#/conditions/type1"},
    },
}

stall_detection_by_two_temps_1_0_0 = {
    "function": function,
    "df_schema": df_schema,
    "settings_jsonschema": settings_jsonschema,
    "aux_jsonschemas": {"#/conditions/type1": condition_jsonschema},
}
