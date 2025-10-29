import logging
from apps.applications.models import Application
from apps.datafeeds.models import Datafeed
from common.complex_types import AppFuncReturn, DerivedDfReadingMap, UpdateMap
from utils.app_func_utils import get_end_rts

logger = logging.getLogger("#monitoring_1_0_0")


def function(
    app: Application, native_df_map: dict[str, Datafeed], derived_df_map: dict[str, Datafeed]
) -> AppFuncReturn:
    """
    This function is for pure monitoring. It doesn't do any calculation and generate
    insigts, it just moves the cursor in order to evaluate the app health.
    """

    logger.info("'monitoring_1_0_0' starts executing...")

    derived_df_reading_map: DerivedDfReadingMap = {}
    update_map: UpdateMap = {}

    # get end time
    start_rts = app.cursor_ts
    end_rts, is_catching_up = get_end_rts(native_df_map.values(), app.time_resample, start_rts, len(native_df_map))

    # update app output
    update_map["cursor_ts"] = end_rts
    update_map["is_catching_up"] = is_catching_up

    return derived_df_reading_map, update_map


df_schema = {
    "[ANY]": {"derived": False, "data_type": "ANY"},
}


monitoring_1_0_0 = {
    "function": function,
    "df_schema": df_schema,
    "settings_jsonschema": {},
    "aux_jsonschemas": {},
}
