from collections import deque
from itertools import islice


# https://stackoverflow.com/questions/10003143/how-to-slice-a-deque
class sliceable_deque(deque):

    def slice(self, start, stop, step=1):
        return type(self)(islice(self, start, stop, step))
