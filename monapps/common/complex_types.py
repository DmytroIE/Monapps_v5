from typing import TypedDict, Literal, Any, Callable
from apps.applications.models import Application
from apps.dfreadings.models import DfReading
from apps.datafeeds.models import Datafeed
from common.constants import HealthGrades


type DfReadingMap = dict[int, dict[int, DfReading]]
type DfValueMap = dict[int, dict[str, int | float]]
type IndDfReadingMap = dict[int, DfReading]

type AlarmPayloadDictForTs = dict[str, Any]  # can be {"CPU Error": {"st": "in"}} or {"CPU Error": {} - can be anything}
type ReevalFields = Literal["status", "curr_state", "health"]


class AlarmRecord(TypedDict):
    persist: bool
    st: Literal["in", "out"]
    lastTransTs: int
    lastInPayloadTs: int


class AlarmMap(TypedDict):  # is stored inside an app, a datastream or a device
    errors: dict[str, AlarmRecord]
    warnings: dict[str, AlarmRecord]


class UpdateMap(TypedDict, total=False):
    cursor_ts: int
    is_catching_up: bool
    health: HealthGrades
    alarm_payload: dict
    state: dict


class DerivedDfReadingRow(TypedDict):
    df: Datafeed
    new_df_readings: list[DfReading]


type DerivedDfReadingMap = dict[str, DerivedDfReadingRow]
type AppFuncReturn = tuple[DerivedDfReadingMap, UpdateMap]

type AppFunction = Callable[[Application, dict[str, Datafeed], dict[str, Datafeed]], AppFuncReturn]
