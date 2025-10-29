from enum import IntEnum
from typing import Callable
from common.constants import CurrStateTypes, HealthGrades
from app_functions.helpers.utils.counters import OnDelayCounter, PlcLikeCounter


class CurrStateAutomataType1:
    """
    This class is used to implement a finite automata that assigns current state to WARNING based on a single condition.
    Also this automata assumes having the 'Off' state."""

    class States(IntEnum):
        OFF = 0
        UNDEFINED = 1
        OK = 2
        WARNING = 3
        ERROR = 4

    def __init__(
        self,
        state: "CurrStateAutomataType1.States",
        prev_state: "CurrStateAutomataType1.States",
        add_to_alarm_payload: Callable,
        count_thres: int,
        counter_type: type[PlcLikeCounter] = OnDelayCounter,
        err_counts: int = 0,
        off_counts: int = 0,
        ok_counts: int = 0,
        warn_counts: int = 0,
    ) -> None:
        self.state = state
        self.prev_state = prev_state
        self.err_counter = counter_type(err_counts, count_thres)
        self.off_counter = counter_type(off_counts, count_thres)
        self.ok_counter = counter_type(ok_counts, count_thres)
        self.warn_counter = counter_type(warn_counts, count_thres)
        self.curr_state = CurrStateTypes.UNDEFINED
        self.health_from_app = HealthGrades.UNDEFINED
        self.add_to_alarm_payload = add_to_alarm_payload

    def execute(self, rts: int, err_flag: bool, off_flag: bool, ok_flag: bool, warn_flag: bool):

        self.err_counter.tick(err_flag)
        self.off_counter.tick(off_flag)
        self.ok_counter.tick(ok_flag)
        self.warn_counter.tick(warn_flag)

        while True:
            again = False
            self.health_from_app = HealthGrades.UNDEFINED
            match self.state:
                case CurrStateAutomataType1.States.OFF:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.err_counter.out:
                        self.state = CurrStateAutomataType1.States.ERROR
                        again = True
                    elif not self.off_counter.out:
                        self.state = CurrStateAutomataType1.States.UNDEFINED
                        again = True
                    else:
                        # permanent actions
                        self.curr_state = CurrStateTypes.UNDEFINED

                case CurrStateAutomataType1.States.UNDEFINED:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.err_counter.out:
                        self.state = CurrStateAutomataType1.States.ERROR
                        again = True
                    elif self.off_counter.out:
                        self.state = CurrStateAutomataType1.States.OFF
                        again = True
                    elif self.warn_counter.out:
                        self.state = CurrStateAutomataType1.States.WARNING
                        again = True
                    elif self.ok_counter.out:
                        self.state = CurrStateAutomataType1.States.OK
                        again = True
                    else:
                        # permanent actions
                        self.curr_state = CurrStateTypes.UNDEFINED

                case CurrStateAutomataType1.States.ERROR:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if not self.err_counter.out:
                        self.state = CurrStateAutomataType1.States.UNDEFINED
                        again = True
                    else:
                        # permanent actions
                        self.health_from_app = HealthGrades.ERROR
                        self.add_to_alarm_payload("Bad input data", {}, rts, "e")
                        # print(f'{create_dt_from_ts_ms(rts)} add_to_alarm_payload("Bad input data")')
                        self.curr_state = CurrStateTypes.UNDEFINED

                case CurrStateAutomataType1.States.OK:
                    # entry actions
                    if self.prev_state != self.state:
                        # do something
                        self.prev_state = self.state

                    # transitions
                    if self.err_counter.out:
                        self.state = CurrStateAutomataType1.States.ERROR
                        again = True
                    elif self.off_counter.out:
                        self.state = CurrStateAutomataType1.States.OFF
                        again = True
                    elif self.warn_counter.out:
                        self.state = CurrStateAutomataType1.States.WARNING
                        again = True
                    else:
                        # permanent actions
                        self.curr_state = CurrStateTypes.OK

                case CurrStateAutomataType1.States.WARNING:
                    # entry actions
                    if self.prev_state != self.state:
                        self.prev_state = self.state

                    # transitions
                    if self.err_counter.out:
                        self.state = CurrStateAutomataType1.States.ERROR
                        again = True
                    elif self.off_counter.out:
                        self.state = CurrStateAutomataType1.States.OFF
                        again = True
                    elif self.ok_counter.out:
                        self.state = CurrStateAutomataType1.States.OK
                        again = True
                    else:
                        # permanent actions
                        self.curr_state = CurrStateTypes.WARNING
                        self.add_to_alarm_payload("Stall detected", {}, rts, "w")
                        # print(f'{create_dt_from_ts_ms(rts)} add_to_alarm_payload("Stall detected")')

            if not again:
                break
