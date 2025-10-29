from enum import IntEnum
from typing import Callable
from common.constants import StatusTypes
from app_functions.helpers.automatas.automata_conditions import ConditionType1
from app_functions.helpers.utils.occ_cluster_list import OccurrenceClusterList


class StatusAutomataType1:
    """
    This type is based on durations of such types of current state - UNDEFINED, OK, WARNING.
    The number of occurrences of current state is not used.
    """

    class States(IntEnum):
        UNDEFINED = 0
        OK = 1
        WARNING = 2
        ERROR = 3

    def __init__(
        self,
        state: "StatusAutomataType1.States",
        prev_state: "StatusAutomataType1.States",
        add_to_alarm_payload: Callable,
        undef_cond: ConditionType1,
        ok_from_undef_cond: ConditionType1,
        ok_from_warn_cond: ConditionType1,
        warn_cond: ConditionType1,
    ) -> None:
        self.state = state
        self.prev_state = prev_state
        self.undef_cond = undef_cond
        self.ok_from_undef_cond = ok_from_undef_cond
        self.ok_from_warn_cond = ok_from_warn_cond
        self.warn_cond = warn_cond
        self.status = StatusTypes.UNDEFINED
        self.add_to_alarm_payload = add_to_alarm_payload

    def execute(self, rts: int, all_occs: OccurrenceClusterList):
        while True:
            again = False
            match self.state:

                case StatusAutomataType1.States.UNDEFINED:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.ok_from_undef_cond.match(all_occs):
                        # optimistic approach - we try to assign OK status as soon as possible
                        # if there are no warning conditions
                        self.state = StatusAutomataType1.States.OK
                        again = True
                    elif self.warn_cond.match(all_occs):
                        self.state = StatusAutomataType1.States.WARNING
                        again = True
                    else:
                        # permanent actions
                        self.status = StatusTypes.UNDEFINED

                case StatusAutomataType1.States.OK:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.warn_cond.match(all_occs):
                        self.state = StatusAutomataType1.States.WARNING
                        again = True
                    elif self.undef_cond.match(all_occs):
                        self.state = StatusAutomataType1.States.UNDEFINED
                        again = True
                    else:
                        # permanent actions
                        self.status = StatusTypes.OK

                case StatusAutomataType1.States.WARNING:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.ok_from_warn_cond.match(all_occs):
                        self.state = StatusAutomataType1.States.OK
                        again = True
                    elif self.undef_cond.match(all_occs):
                        self.state = StatusAutomataType1.States.UNDEFINED
                        again = True
                    else:
                        # permanent actions
                        self.status = StatusTypes.WARNING
            if not again:
                break
