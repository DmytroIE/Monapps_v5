from typing import TypedDict, Literal, Any

from common.constants import StatusTypes
from app_functions.helpers.utils.occ_cluster_list import OccurrenceClusterList

type CondLiteral = Literal[">", "<", ">=", "<=", "==", "!="]


def eval_cond(first: Any, cond: CondLiteral, second: Any) -> bool:
    if cond == "==":
        return first == second
    elif cond == "!=":
        return first != second
    elif cond == ">":
        return first > second
    elif cond == ">=":
        return first >= second
    elif cond == "<":
        return first < second
    elif cond == "<=":
        return first <= second


class InitDictForConditionType1(TypedDict):
    total_occs: int
    ok_cond: CondLiteral
    num_of_ok_occs: int
    warn_cond: CondLiteral
    num_of_warn_occs: int
    undef_cond: CondLiteral
    num_of_undef_occs: int


class ConditionType1(dict):
    """
    The dict-like object that describes conditions for state transitions of StatusAutomataType1.
    These conditions are based on numbers of occurrences of current state within an interval.
    Conditions are joint with logical "and" in the "match" method.
    """

    def __init__(self, init_dict: InitDictForConditionType1) -> None:
        if (init_dict["num_of_ok_occs"] + init_dict["num_of_warn_occs"] + init_dict["num_of_undef_occs"]) > init_dict[
            "total_occs"
        ]:
            raise ValueError("num_of_ok_occs + num_of_warn_occs + num_of_undef_occs > total_occs")
        self.update(init_dict)

    def match(self, occs: OccurrenceClusterList) -> bool:
        last_occs = occs.get_slice_with_last_n_occurrences(self["total_occs"])
        num_of_ok_occs = last_occs.count_occurrences_of_value(StatusTypes.OK)
        num_of_undef_occs = last_occs.count_occurrences_of_value(StatusTypes.UNDEFINED)
        num_of_warn_occs = last_occs.count_occurrences_of_value(StatusTypes.WARNING)
        return (
            eval_cond(num_of_ok_occs, self["ok_cond"], self["num_of_ok_occs"])
            and eval_cond(num_of_undef_occs, self["undef_cond"], self["num_of_undef_occs"])
            and eval_cond(num_of_warn_occs, self["warn_cond"], self["num_of_warn_occs"])
        )
