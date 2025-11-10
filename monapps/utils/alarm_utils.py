from typing import Literal, Callable
import copy

from apps.applications.models import Application
from apps.datastreams.models import Datastream
from apps.devices.models import Device

from common.complex_types import AlarmPayloadDictForTs
from services.alarm_log import add_to_alarm_log


def add_to_alarm_payload(alarm_payload: dict, alarm_name: str, alarm_dict: dict, ts: int, key: Literal["e", "w", "i"]):
    """This function is used by app functions, it helps to shape the alarm payload similar to what
    comes from datastreams and devices.
    {
        1734567890123: {"e":{"CPU Error": {"st": "in"}, "Another error": {}}, "w": {"Some warning": {}}},
        1734567890345: {...},
        ...
        }
    This payload then can be processed by the 'update_alarm_map' function.
    """
    if ts not in alarm_payload:
        alarm_payload[ts] = {}

    if key not in alarm_payload[ts]:
        if key == "i":
            alarm_payload[ts][key] = [alarm_name]
        else:
            alarm_payload[ts][key] = {alarm_name: alarm_dict}
            # will look like {"CPU Error": {"st": "in"}} or {"CPU Error": {}}
    else:
        if key == "i":
            alarm_payload[ts][key].append(alarm_name)
        else:
            alarm_payload[ts][key].update(alarm_name=alarm_dict)


def at_least_one_alarm_in(alarm_map):
    """Estimates if at least one alarm has status "in"."""
    at_least_one_in = False
    for alarm in alarm_map.values():
        if alarm.get("st") == "in":
            at_least_one_in = True
            break
    return at_least_one_in


type AddToLogFunc = Callable[
    [Literal["ERROR", "WARNING", "INFO"], str, int, Device | Datastream | Application, str], None
]


def update_alarm_map(
    instance: Device | Datastream | Application,
    alarm_dict: AlarmPayloadDictForTs | None,
    ts: int,
    alarm_map_type: Literal["errors", "warnings"],
    has_value: bool = False,
    add_to_log: AddToLogFunc = add_to_alarm_log,
):
    """
    This function updates certain type of an alarm map - "errors" or "warnings". Also, it puts all
    the transistions of individual alarms into the alarm log. The variable "alarm_dict_for_ts" should contain
    the dictionary of errors or warnings for a certain timestamp. This dictionary looks like
    {"error 1 name": {"st": "in"}, "error 2 name ": {}, ...} and usually is part of payload coming from different
    devices and datastreams. Errors and warnings that have the field "st" equal to "in" are persistent - you
    can send them only once, when the alarm is triggered, and it will last until it is cleared with {"st": "out"}.
    For persistent errors there is one more way to clear - if the "has_value" parameter is True,
    and there is no record in parallel for this particular timestamp. It means that if there is value
    and no error for a certain timestamp, then the error is no longer relevant (otherwise there wouldn't be
    a value).
    """
    alarm_map = getattr(instance, alarm_map_type)
    log_level: Literal["ERROR", "WARNING"] = alarm_map_type[:-1].upper()
    is_nd_marker_needed = False
    upd_alarm_map = copy.deepcopy(alarm_map)
    if alarm_dict is not None:
        for alarm_name, ind_alarm_obj in alarm_dict.items():  # ind_alarm_obj can be {"st": "in"} or {}
            if alarm_name in upd_alarm_map:
                if isinstance(ind_alarm_obj, dict) and (
                    (new_status := str(ind_alarm_obj.get("st")).lower()) == "in" or new_status == "out"
                ):
                    upd_alarm_map[alarm_name]["persist"] = True
                    upd_alarm_map[alarm_name]["lastInPayloadTs"] = ts
                    # it is not reasonable to create an nd marker every time
                    # when the same persistent alarm with the status "in" comes
                    # but if there is also a value in parallel, then an nd marker
                    # should be created
                    if alarm_map_type == "errors" and new_status == "in" and has_value:
                        is_nd_marker_needed = True
                    # print(f"Persistent alarm '{alarm_name}' comes again with st='{new_status}'")
                    if upd_alarm_map[alarm_name]["st"] != new_status:
                        upd_alarm_map[alarm_name]["st"] = new_status
                        upd_alarm_map[alarm_name]["lastTransTs"] = ts
                        add_to_log(log_level, alarm_name, ts, instance, new_status)
                        # also, an nd marker should be created when the alarm
                        # emerges first time after being "out"
                        if alarm_map_type == "errors" and new_status == "in":
                            is_nd_marker_needed = True
                else:
                    upd_alarm_map[alarm_name]["persist"] = False
                    upd_alarm_map[alarm_name]["lastInPayloadTs"] = ts
                    # it is not reasonable to create an nd marker every time
                    # when the same non-persistent alarm comes
                    # but if there is also a value in parallel, then an nd marker
                    # should be created
                    if alarm_map_type == "errors" and has_value:
                        is_nd_marker_needed = True
                    # print(f"NON-Persistent alarm '{alarm_name}' comes again with st='in'")
                    if upd_alarm_map[alarm_name]["st"] != "in":
                        upd_alarm_map[alarm_name]["st"] = "in"
                        upd_alarm_map[alarm_name]["lastTransTs"] = ts
                        add_to_log(log_level, alarm_name, ts, instance, "in")
                        # also, an nd marker should be created when the alarm
                        # emerges first time after being "out"
                        if alarm_map_type == "errors":
                            is_nd_marker_needed = True

            else:
                upd_alarm_map[alarm_name] = {}
                if isinstance(ind_alarm_obj, dict) and (
                    (new_status := str(ind_alarm_obj.get("st")).lower()) == "in" or new_status == "out"
                ):
                    upd_alarm_map[alarm_name]["persist"] = True
                    upd_alarm_map[alarm_name]["st"] = new_status
                    upd_alarm_map[alarm_name]["lastInPayloadTs"] = ts
                    upd_alarm_map[alarm_name]["lastTransTs"] = ts  # an arguable question what to put here when "out"
                    # print(f"Persistent alarm '{alarm_name}' comes FIRST TIME with st='{new_status}'")
                    if new_status == "in":  # if the first message has the status "out", then no sense in logging it
                        add_to_log(log_level, alarm_name, ts, instance, "in")
                        if alarm_map_type == "errors":
                            is_nd_marker_needed = True
                else:
                    upd_alarm_map[alarm_name]["persist"] = False
                    upd_alarm_map[alarm_name]["st"] = "in"
                    upd_alarm_map[alarm_name]["lastInPayloadTs"] = ts
                    upd_alarm_map[alarm_name]["lastTransTs"] = ts
                    # print(f"NON-Persistent alarm '{alarm_name}' comes FIRST TIME with st='in'")
                    add_to_log(log_level, alarm_name, ts, instance, "in")
                    if alarm_map_type == "errors":
                        is_nd_marker_needed = True

    for alarm_name, ind_alarm_obj in upd_alarm_map.items():
        # if there is at least one datastream with a value and without an error,
        # all persistent errors get discarded, otherwise, it acquires "out" in the upper part of the code
        if ind_alarm_obj["persist"]:
            if (
                alarm_map_type == "errors"
                and ind_alarm_obj["st"] == "in"
                and ind_alarm_obj["lastInPayloadTs"] < ts
                and has_value
            ):
                ind_alarm_obj["st"] = "out"
                ind_alarm_obj["lastTransTs"] = ts
                # print(f"Persistent alarm '{alarm_name}' st='out' because at least one ds has readings and no errors")
                add_to_log(log_level, alarm_name, ts, instance, "out")
        else:
            # non-persisten alarms acquire "out" when there is no such an alarm in 'alarm_dict_for_ts'
            if ind_alarm_obj["st"] == "in" and (alarm_dict is None or alarm_dict.get(alarm_name) is None):
                ind_alarm_obj["st"] = "out"
                ind_alarm_obj["lastTransTs"] = ts
                # print(f"NON-Persistent alarm '{alarm_name}' st='out' because no fresh alarm incoming readings")
                add_to_log(log_level, alarm_name, ts, instance, "out")

    return upd_alarm_map, is_nd_marker_needed
