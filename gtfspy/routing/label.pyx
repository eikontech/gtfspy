cdef class LabelTimeSimple:
    """
    LabelTime describes entries in a Profile.
    """
    cdef:
        public double departure_time
        public double arrival_time_target

    def __init__(self, departure_time=-float("inf"), arrival_time_target=float('inf')):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target

    def __richcmp__(LabelTimeSimple self, LabelTimeSimple other, int op):
        self_tuple = self.departure_time, -self.arrival_time_target
        other_tuple = other.departure_time, -other.arrival_time_target
        if op == 2: # ==
            return self_tuple == other_tuple
        if op == 3: # !=
            return self_tuple != other_tuple
        if op == 0: # less than
            return self_tuple < other_tuple
        elif op == 4: # greater than
            return self_tuple > other_tuple
        elif op == 1: # <=
            return self_tuple <= other_tuple
        elif op == 5: # >=
            return self_tuple >= other_tuple

    # __getstate__ and __setstate__ : for pickling
    def __getstate__(self):
        return self.departure_time, self.arrival_time_target

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target = state

    cpdef int dominates(self, LabelTimeSimple other) except *:
        return self.departure_time >= other.departure_time and self.arrival_time_target <= other.arrival_time_target

    cpdef int dominates_ignoring_dep_time(self, LabelTimeSimple other):
        return self.arrival_time_target <= other.arrival_time_target

    @staticmethod
    def direct_walk_label(float departure_time, float walk_duration):
        return LabelTimeSimple(departure_time, departure_time + walk_duration)

    cpdef float duration(self):
        return self.arrival_time_target - self.departure_time

    cpdef LabelTimeSimple get_copy(self):
        return LabelTimeSimple(self.departure_time, self.arrival_time_target)

cdef class LabelTime:
    """
    LabelTime describes entries in a Profile.
    """
    cdef:
        public double departure_time
        public double arrival_time_target
        public bint first_leg_is_walk

    def __init__(self, departure_time=-float("inf"), arrival_time_target=float('inf'), bint first_leg_is_walk=False,
                 **kwargs):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.first_leg_is_walk = first_leg_is_walk

    cpdef _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, not self.first_leg_is_walk

    def __richcmp__(LabelTime self, LabelTime other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2: # ==
            return self_tuple == other_tuple
        if op == 3: # !=
            return self_tuple != other_tuple
        if op == 0: # less than
            return self_tuple < other_tuple
        elif op == 4: # greater than
            return self_tuple > other_tuple
        elif op == 1: # <=
            return self_tuple <= other_tuple
        elif op == 5: # >=
            return self_tuple >= other_tuple

    # __getstate__ and __setstate__ : for pickling
    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.first_leg_is_walk

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.first_leg_is_walk = state

    cpdef int dominates(LabelTime self, LabelTime other) except *:
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        return all([(s >= o) for s, o in zip(self_tuple,other_tuple)])

    cpdef int dominates_ignoring_dep_time(LabelTime self, LabelTime other):
        return self.arrival_time_target <= other.arrival_time_target and self.first_leg_is_walk <= other.first_leg_is_walk

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTime other):
        return self.arrival_time_target <= other.arrival_time_target

    cpdef LabelTime get_copy(self):
        return LabelTime(self.departure_time, self.arrival_time_target, self.first_leg_is_walk)

    cpdef LabelTime get_copy_with_specified_departure_time(self, float departure_time):
        return LabelTime(departure_time, self.arrival_time_target, self.first_leg_is_walk)

    @staticmethod
    def direct_walk_label(float departure_time, float walk_duration):
        return LabelTime(departure_time, departure_time + walk_duration, True)

    cpdef float duration(self):
        return self.arrival_time_target - self.departure_time

    cpdef LabelTime get_copy_with_walk_added(self, walk_duration):
        return LabelTime(self.departure_time - walk_duration, self.arrival_time_target, self.first_leg_is_walk)


cdef class LabelTimeAndVehLegCount:
    cdef:
        public double departure_time
        public double arrival_time_target
        public int n_vehicle_legs
        public bint first_leg_is_walk


    def __init__(self, float departure_time, float arrival_time_target,
                 int n_vehicle_legs, bint first_leg_is_walk):
        self.departure_time = departure_time
        self.arrival_time_target = arrival_time_target
        self.n_vehicle_legs = n_vehicle_legs
        self.first_leg_is_walk = first_leg_is_walk

    def __getstate__(self):
        return self.departure_time, self.arrival_time_target, self.n_vehicle_legs

    def __setstate__(self, state):
        self.departure_time, self.arrival_time_target, self.n_vehicle_legs = state

    def _tuple_for_ordering(self):
        return self.departure_time, -self.arrival_time_target, -self.n_vehicle_legs, not self.first_leg_is_walk

    def __richcmp__(LabelTimeAndVehLegCount self, LabelTimeAndVehLegCount other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2: # ==
            return self_tuple == other_tuple
        if op == 3: # !=
            return self_tuple != other_tuple
        if op == 0: # less than
            return self_tuple < other_tuple
        elif op == 4: # greater than
            return self_tuple > other_tuple
        elif op == 1: # <=
            return self_tuple <= other_tuple
        elif op == 5: # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelTimeAndVehLegCount other):
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles

        Parameters
        ----------
        other: LabelTimeAndVehLegCount

        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        return all([(s >= o) for s, o in zip(self_tuple,other_tuple)])

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelTimeAndVehLegCount other):
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.n_vehicle_legs <= other.n_vehicle_legs
        )
        return dominates

    cpdef int dominates_ignoring_dep_time(self, LabelTimeAndVehLegCount other):
        cdef:
            int dominates
        dominates = (
            self.arrival_time_target <= other.arrival_time_target and
            self.n_vehicle_legs <= other.n_vehicle_legs and
            self.first_leg_is_walk <= other.first_leg_is_walk
        )
        return dominates

    cpdef get_copy(self):
        return LabelTimeAndVehLegCount(self.departure_time, self.arrival_time_target,
                                       self.n_vehicle_legs, self.first_leg_is_walk)

    cpdef get_copy_with_specified_departure_time(self, departure_time):
        return LabelTimeAndVehLegCount(departure_time, self.arrival_time_target,
                                       self.n_vehicle_legs, self.first_leg_is_walk)

    cpdef float duration(self):
        return self.arrival_time_target - self.departure_time

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelTimeAndVehLegCount(departure_time, departure_time + walk_duration, 0, True)

    cpdef LabelTimeAndVehLegCount get_copy_with_walk_added(self, float walk_duration):
        return LabelTimeAndVehLegCount(self.departure_time - walk_duration,
                                       self.arrival_time_target, self.n_vehicle_legs, True)

    def __str__(self):
        return str((self.departure_time, self.arrival_time_target, self.n_vehicle_legs, self.first_leg_is_walk))



cdef class LabelVehLegCount:
    cdef:
        public double departure_time
        public int n_vehicle_legs
        public bint first_leg_is_walk

    def __init__(self, n_vehicle_legs=0, departure_time=-float('inf'), first_leg_is_walk=False, **kwargs):
        self.n_vehicle_legs = n_vehicle_legs
        self.departure_time = departure_time
        self.first_leg_is_walk = first_leg_is_walk

    def __getstate__(self):
        return self.departure_time, self.n_vehicle_legs, self.first_leg_is_walk

    def __setstate__(self, state):
        self.departure_time, self.n_vehicle_legs, self.first_leg_is_walk = state

    def _tuple_for_ordering(self):
        return -self.n_vehicle_legs, self.departure_time, not self.first_leg_is_walk

    def __richcmp__(LabelVehLegCount self, LabelVehLegCount other, int op):
        self_tuple = self._tuple_for_ordering()
        other_tuple = other._tuple_for_ordering()
        if op == 2:
            return self_tuple == other_tuple
        if op == 3:
            return self_tuple != other_tuple
        if op == 0: # less than
            return self_tuple < other_tuple
        elif op == 4: # greater than
            return self_tuple > other_tuple
        elif op == 1: # <=
            return self_tuple <= other_tuple
        elif op == 5: # >=
            return self_tuple >= other_tuple

    cpdef int dominates(self, LabelVehLegCount other) except *:
        """
        Compute whether this LabelWithNumberVehicles dominates the other LabelWithNumberVehicles

        Parameters
        ----------
        other: LabelTimeAndVehLegCount

        Returns
        -------
        dominates: bint
            True if this ParetoTuple dominates the other, otherwise False
        """
        return self.n_vehicle_legs <= other.n_vehicle_legs and self.first_leg_is_walk <= other.first_leg_is_walk

    cpdef int dominates_ignoring_dep_time(self, LabelVehLegCount other):
        return self.dominates(other)

    cpdef int dominates_ignoring_dep_time_finalization(self, LabelVehLegCount other):
        return self.n_vehicle_legs <= other.n_vehicle_legs

    def get_copy(self):
        return LabelVehLegCount(self.n_vehicle_legs, first_leg_is_walk=self.first_leg_is_walk)

    def get_copy_with_specified_departure_time(self, departure_time):
        return LabelVehLegCount(self.n_vehicle_legs, departure_time, self.first_leg_is_walk)

    def get_copy_with_walk_added(self, walk_duration):
        return LabelVehLegCount(self.n_vehicle_legs, departure_time=self.departure_time - walk_duration,
                                first_leg_is_walk=True)

    @staticmethod
    def direct_walk_label(departure_time, walk_duration):
        return LabelVehLegCount(0, departure_time, True)

# ctypedef fused label:
#     LabelTime
#     LabelTimeAndVehLegCount
#     LabelVehLegCount
#
# cpdef int dominates(label first, label other):
#     return first.dominates(other)

def compute_pareto_front_smart(list label_list):
    return compute_pareto_front(label_list)

def compute_pareto_front(list label_list, finalization=False):
    pareto_front = []
    if len(label_list) == 0:
        return pareto_front

    # determine function used for domination:
    label = next(iter(label_list))
    if finalization:
        dominates = label.__class__.dominates_ignoring_dep_time_finalization
    else:
        dominates = label.__class__.dominates_ignoring_dep_time

    label_list = list(reversed(sorted(label_list))) # n log(n)
    # assume only that label_list is sorted by departure time (best last)
    current_best_labels_wo_deptime = []
    for new_label in label_list: # n times
        is_dominated = False
        for best_label in current_best_labels_wo_deptime:
            # the size of current_best_labels_wo_deptime should remain small
            # the new_label can dominate the old ones only partially
            # check if the new one is dominated by the old ones ->
            if dominates(best_label, new_label):
                is_dominated = True
                break
        if is_dominated:
            continue # do nothing
        else:
            pareto_front.append(new_label)
            new_best = []
            for old_partial_best in current_best_labels_wo_deptime:
                if not dominates(new_label, old_partial_best):
                    new_best.append(old_partial_best)
            new_best.append(new_label)
            current_best_labels_wo_deptime = new_best
    return pareto_front


def compute_pareto_front_naive(list label_list):
    """
    Computes the Pareto frontier of a given label_list

    Parameters
    ----------
    label_list: list[LabelTime]
        (Or any list of objects, for which a function label.dominates(other) is defined.

    Returns
    -------
    pareto_front: list[LabelTime]
        List of labels that belong to the Pareto front.

    Notes
    -----
    Code adapted from:
    http://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
    """
    # cdef:
    #    list dominated
    #    list pareto_front
    #    list remaining
    #    Label other
    #    Label candidate
    dominated = []
    pareto_front = []
    remaining = label_list
    while remaining:  # (is not empty)
        candidate = remaining[0]
        new_remaining = []
        is_dominated = False
        for other_i in range(1, len(remaining)):
            other = remaining[other_i]
            if candidate.dominates(other):
                dominated.append(other)
            else:
                new_remaining.append(other)
                if other.dominates(candidate):
                    is_dominated = True
        if is_dominated:
            dominated.append(candidate)
        else:
            pareto_front.append(candidate)
        remaining = new_remaining
        # after each round:
        #   remaining contains Labels that are not dominated by any in the pareto_front
        #   dominated contains Labels that are dominated by some other LabelTime
    return pareto_front

#

def merge_pareto_frontiers(labels, labels_other):
    """
    Merge two pareto frontiers by removing dominated entries.

    Parameters
    ----------
    labels: list[LabelTime]
    labels_other: list[LabelTime]

    Returns
    -------
    pareto_front_merged: list[LabelTime]
    """
    def _get_non_dominated_entries(candidates, possible_dominators, survivor_list=None):
        if survivor_list is None:
            survivor_list = list()
        for candidate in candidates:
            candidate_is_dominated = False
            for dominator in possible_dominators:
                if dominator.dominates(candidate):
                    candidate_is_dominated = True
                    break
            if not candidate_is_dominated:
                survivor_list.append(candidate)
        return survivor_list

    survived = []
    survived = _get_non_dominated_entries(labels, labels_other, survivor_list=survived)
    survived = _get_non_dominated_entries(labels_other, survived, survivor_list=survived)
    return survived


def min_arrival_time_target(label_list):
    if len(label_list) > 0:
        return min(label_list, key=lambda label: label.arrival_time_target).arrival_time_target
    else:
        return float('inf')


def min_n_vehicle_trips(label_list):
    if len(label_list) > 0:
        return min(label_list, key=lambda label: label.n_vehicle_trips).n_vehicle_trips
    else:
        return None