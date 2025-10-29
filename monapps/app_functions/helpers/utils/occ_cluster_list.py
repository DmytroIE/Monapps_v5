class OccurrenceClusterList(list):
    """
    List of lists [value, count]. It may look like [[1:33], [2:4], [3:18], [1: 15], [3: 22]...].
    """

    def __init__(self, init_list=None):
        super().__init__()
        if init_list is not None:
            self.extend(init_list)

    def get_total_occurrences(self):
        num_of_occs = 0
        for item in self:
            num_of_occs += item[1]
        return num_of_occs

    def append_occurrence(self, value: int | str) -> None:
        if len(self) > 0 and self[len(self) - 1][0] == value:
            self[len(self) - 1][1] += 1
            return
        self.append([value, 1])

    def count_occurrences_of_value(self, value: int | str) -> int:
        num = 0
        for item in self:
            if item[0] == value:
                num += item[1]
        return num

    def get_slice_with_last_n_occurrences(self, number_of_occurrences) -> "OccurrenceClusterList":
        sliced = []
        for i in range(len(self) - 1, -1, -1):
            if number_of_occurrences - self[i][1] >= 0:
                sliced.append(self[i])
                number_of_occurrences -= self[i][1]
            elif number_of_occurrences > 0 and number_of_occurrences - self[i][1] < 0:
                sliced.append([self[i][0], number_of_occurrences])
                number_of_occurrences = 0
            else:
                break
        return type(self)(reversed(sliced))
