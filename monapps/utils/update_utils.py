from typing import Any
from collections.abc import Iterable
from django.conf import settings
from common.constants import StatusTypes, CurrStateTypes, StatusUse, CurrStateUse, HealthGrades


def derive_health_from_children(children_objects: Iterable[Any]) -> HealthGrades:
    # NOTE: "health" will be "WARNING" if not all children are ERROR,
    # also instances with health = "UNDEFINED" will be omitted

    health_assumption = HealthGrades.UNDEFINED
    num_with_health_error = 0
    num_with_health_warn_ok = 0
    all_have_error_health = False

    for obj in children_objects:
        if not hasattr(obj, "health"):
            continue
        if obj.health == HealthGrades.OK or obj.health == HealthGrades.WARNING:
            num_with_health_warn_ok += 1
        elif obj.health == HealthGrades.ERROR:
            num_with_health_error += 1

    if num_with_health_warn_ok == 0 and num_with_health_error > 0:
        all_have_error_health = True

    if all_have_error_health:
        health_assumption = HealthGrades.ERROR
    else:
        for obj in children_objects:
            if not hasattr(obj, "health"):
                continue
            if obj.health > health_assumption:
                if obj.health == HealthGrades.ERROR and not all_have_error_health:
                    health_assumption = HealthGrades.WARNING
                else:
                    health_assumption = obj.health
    return health_assumption


def derive_status_from_children(
    children_objects: Iterable[Any],
) -> StatusTypes | None:  # invoked when any children updates its status

    status_assumption = StatusTypes.UNDEFINED
    status_is_none_assumption = True  # if none of children has the status, then the parent doesn't have the status too
    all_have_error_status = False
    num_with_st_error = 0
    num_with_st_warn_ok = 0

    for obj in children_objects:  # first cycle
        if not hasattr(obj, "status"):
            continue
        if obj.status is None or obj.status_use == StatusUse.DONT_USE:
            continue
        if hasattr(obj, "is_status_stale") and obj.is_status_stale:
            # this will turn the status of the parent to UNDEFINED if there are only children
            # with stale statuses
            status_is_none_assumption = False
            continue

        status_is_none_assumption = False
        if obj.status == StatusTypes.OK or obj.status == StatusTypes.WARNING:  # StatusTypes.UNDEFINED is not used here
            num_with_st_warn_ok += 1
        elif obj.status == StatusTypes.ERROR:
            num_with_st_error += 1

    if num_with_st_warn_ok == 0 and num_with_st_error > 0:
        all_have_error_status = True

    if not status_is_none_assumption:
        for obj in children_objects:  # second cycle
            if not hasattr(obj, "status"):
                continue
            if (
                obj.status is None
                or obj.status_use == StatusUse.DONT_USE
                or (hasattr(obj, "is_status_stale") and obj.is_status_stale)
            ):
                continue
            else:
                if obj.status > status_assumption:
                    if obj.status == StatusTypes.ERROR:
                        if (obj.status_use == StatusUse.AS_WARNING) or (
                            obj.status_use == StatusUse.AS_ERROR_IF_ALL and not all_have_error_status
                        ):
                            status_assumption = StatusTypes.WARNING
                        else:
                            status_assumption = obj.status
                    else:
                        status_assumption = obj.status
    else:  # if the status was switched to None in every child
        status_assumption = None  # then the parent will also aquire the status = None

    return status_assumption


def derive_curr_state_from_children(
    children_objects: Iterable[Any],
) -> CurrStateTypes | None:  # invoked when any children updates its current state

    curr_state_assumption = CurrStateTypes.UNDEFINED
    curr_state_is_none_assumption = (
        True  # if none of children has the current state, then the parent doesn't have the current state too
    )
    all_have_error_curr_state = False
    num_with_cs_error = 0
    num_with_cs_warn_ok = 0

    for obj in children_objects:  # first cycle
        if not hasattr(obj, "curr_state"):
            continue
        if obj.curr_state is None or obj.curr_state_use == CurrStateUse.DONT_USE:
            continue
        if hasattr(obj, "is_curr_state_stale") and obj.is_curr_state_stale:
            curr_state_is_none_assumption = False
            continue

        curr_state_is_none_assumption = False
        if obj.curr_state == CurrStateTypes.OK or obj.curr_state == CurrStateTypes.WARNING:
            num_with_cs_warn_ok += 1
        elif obj.curr_state == CurrStateTypes.ERROR:
            num_with_cs_error += 1

    if num_with_cs_warn_ok == 0 and num_with_cs_error > 0:
        all_have_error_curr_state = True

    if not curr_state_is_none_assumption:
        for obj in children_objects:  # second cycle
            if not hasattr(obj, "curr_state"):
                continue
            if (
                obj.curr_state is None
                or obj.curr_state_use == CurrStateUse.DONT_USE
                or (hasattr(obj, "is_curr_state_stale") and obj.is_curr_state_stale)
            ):
                continue
            else:
                if obj.curr_state > curr_state_assumption:
                    if obj.curr_state == CurrStateTypes.ERROR:
                        if obj.curr_state_use == CurrStateUse.AS_WARNING or (
                            obj.curr_state_use == CurrStateUse.AS_ERROR_IF_ALL and not all_have_error_curr_state
                        ):
                            curr_state_assumption = CurrStateTypes.WARNING
                        else:
                            curr_state_assumption = obj.curr_state
                    else:
                        curr_state_assumption = obj.curr_state

    else:  # if the current state was switched to None in every child
        curr_state_assumption = None  # the parent will also aquire the current state = None

    return curr_state_assumption


update_func_by_property_map = {
    "status": derive_status_from_children,
    "curr_state": derive_curr_state_from_children,
    "health": derive_health_from_children,
}


def enqueue_update(asset_lnk, now_ts: int, coef=0.8):
    """
    Adjusts the next update time of the asset.

    :param asset_lnk: device/asset instance (will be changed in-place)
    :param now_ts: the timestamp of the moment when the enqueuement is called
    :param coef: the coefficient of the time margin to speed up the update in certain cases
    """
    if asset_lnk is None or not hasattr(asset_lnk, "next_upd_ts"):
        return

    time_margin = int(settings.TIME_ASSET_UPD_MS * coef)
    if asset_lnk.next_upd_ts > now_ts + time_margin:
        asset_lnk.next_upd_ts = now_ts + time_margin
        asset_lnk.update_fields.add("next_upd_ts")


def update_reeval_fields(asset_lnk, fields: str | Iterable[str]):
    """
    Updates the 'reeval_fields' property.

    :param asset_lnk: the parent asset instance (will be changed in-place)
    :param fields: a field / list of fields changed in the child asset that may need to be reevaluated in the parent
    """

    if asset_lnk is None or not hasattr(asset_lnk, "reeval_fields"):
        return
    if isinstance(fields, str):
        fields = [fields]
    for field in fields:
        if field not in asset_lnk.reeval_fields:
            asset_lnk.reeval_fields.append(field)
            asset_lnk.update_fields.add("reeval_fields")  # for the 'save' function


def set_attr_if_cond(new_value, cond, instance, field_name):
    old_value = getattr(instance, field_name)
    if old_value is None and (cond == ">" or cond == "<"):
        old_value = 0
    match cond:
        case ">":
            if new_value <= old_value:
                return False
        case "<":
            if new_value >= old_value:
                return False
        case "!=":
            if new_value == old_value:
                return False
        case _:
            raise ValueError(f"Unknown condition: {cond}")
    setattr(instance, field_name, new_value)
    instance.update_fields.add(field_name)
    return True
