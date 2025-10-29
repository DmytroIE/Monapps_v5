class TimeIntervalMap:
    """
    A map of time intervals. The key is the start of the interval (timestamp expressed in ms)
    and the value is the end of the interval. Looks like
    {1741111111111: 1742222222222, 1743333333333: 1744444444444, ...}"""

    def __init__(self, init_map: dict[int, int] | None = None) -> None:
        if init_map is not None:
            self.map = TimeIntervalMap.condition_map(init_map)
        else:
            self.map: dict[int, int] = {}

    def __str__(self) -> str:
        s = "Time Interval Map: ["
        for start_ts, end_ts in self.map.items():
            s += f"({start_ts}:{end_ts}),"
        s += "]"
        return s

    @staticmethod
    def condition_map(map: dict[int, int]) -> dict[int, int]:
        if len(map) == 0:
            return {}

        map_inter = {}
        for start_ts, end_ts in map.items():
            if start_ts > end_ts:  # if start_ts > end_ts within the same interval, just omit
                start_ts, end_ts = end_ts, start_ts
            map_inter[start_ts] = end_ts

        map_inter = dict(sorted(map_inter.items()))

        prev_int_start_ts = 0
        prev_int_end_ts = 0
        new_map = {}
        for idx, (int_start_ts, int_end_ts) in enumerate(map_inter.items()):
            if int_start_ts > int_end_ts:  # if start_ts > end_ts within the same interval, just omit
                continue
            # if there are overlapping intevals, then "glue" them together
            if idx > 0:  # at least two items
                if prev_int_end_ts >= int_start_ts:  # if end_ts of the previous interval >= start_ts of the current
                    prev_int_end_ts = max(prev_int_end_ts, int_end_ts)
                    new_map[prev_int_start_ts] = prev_int_end_ts
                    continue
            new_map[int_start_ts] = int_end_ts
            prev_int_start_ts = int_start_ts
            prev_int_end_ts = int_end_ts

        return new_map

    def get_info_for_interval(self, ts1: int, ts2: int) -> tuple[int, int]:
        start_ts = min(ts1, ts2)
        end_ts = max(ts1, ts2)
        total_duration = 0
        num_of_occurrences = 0
        for int_start_ts, int_end_ts in self.map.items():
            if int_start_ts > end_ts or int_end_ts < start_ts:
                continue
            if int_start_ts < start_ts:
                int_start_ts = start_ts
            if int_end_ts > end_ts:
                int_end_ts = end_ts
            total_duration += int_end_ts - int_start_ts
            num_of_occurrences += 1
        return total_duration, num_of_occurrences

    def delete_old_intervals(self, end_ts: int) -> None:
        new_map = {}
        for int_start_ts, int_end_ts in self.map.items():
            if int_end_ts < end_ts:  # the unclosed interval is not deleted in any case
                continue
            new_map[int_start_ts] = int_end_ts
        self.map = new_map

    def add_interval(self, ts1: int, ts2: int) -> None:
        start_ts = min(ts1, ts2)
        end_ts = max(ts1, ts2)
        if start_ts in self.map:
            new_map = {**self.map}
            new_map[start_ts] = max(self.map[start_ts], end_ts)  # if start_ts is already in the map, update end_ts
        else:
            new_map = {**self.map, start_ts: end_ts}
        self.map = TimeIntervalMap.condition_map(new_map)

    def get_last_end_ts(self) -> int | None:
        if len(self.map) == 0:
            return None
        return max(self.map.values())
