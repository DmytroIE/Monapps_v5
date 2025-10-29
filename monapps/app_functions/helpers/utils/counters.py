class PlcLikeCounter:
    def __init__(self, initial: int = 0, preset: int = 1) -> None:
        self.counts = initial
        self._preset = preset if preset > 0 else 1
        self.out = False

    def reset(self) -> None:
        self.counts = 0
        self.out = False

    def tick(self, cond: bool) -> None:
        raise NotImplementedError


class OnDelayCounter(PlcLikeCounter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def tick(self, cond: bool) -> None:
        if cond:
            self.counts += 1
            if self.counts >= self._preset:
                self.counts = self._preset
                self.out = True
        else:
            self.counts = 0
            self.out = False
