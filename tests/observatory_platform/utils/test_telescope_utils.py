# Copyright 2019 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Tuan Chien

import pendulum
import unittest

from observatory_platform.utils.telescope_utils import (
    PeriodCount,
    ScheduleOptimiser
)


class TestScheduleOptimiser(unittest.TestCase):
    """ Test schedule optimiser that minimises API calls. """

    def __init__(self, *args, **kwargs):
        super(TestScheduleOptimiser, self).__init__(*args, **kwargs)

        self.max_per_call = 2
        self.max_per_query = 10

        self.historic_counts_trivial = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

    def test_get_num_calls(self):
        """ Test the num_call calculation. """

        num_results = 10
        max_per_call = 2
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 5)

        max_per_call = 3
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 4)

    def test_extract_schedule(self):
        """ Test schedule extraction from solution. """

        moves = [0, 1, 2, 3]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)

        for i in range(len(schedule), 0, -1):
            self.assertEqual(schedule[i - 1].start.month, i)

        moves = [0, 1, 2, 1]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_leading_zeros(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 21)), 1),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 4, 21))
        self.assertEqual(min_calls, 1)
        self.assertEqual(len(schedule), 1)

    def test_optimise_leading_zeros2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 0)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)

    def test_optimise_leading_trivial(self):
        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query,
                                                         self.historic_counts_trivial)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 3)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 4)

    def test_optimise_historic_counts_case1(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(min_calls, 8)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_historic_counts_case2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 5),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 6),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 4)  # Naive is 5
        self.assertEqual(min_calls, 12)  # Naive is 12
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[2].start, pendulum.datetime(1000, 3, 1))
        self.assertEqual(schedule[2].end, pendulum.datetime(1000, 4, 1))
        self.assertEqual(schedule[3].start, pendulum.datetime(1000, 5, 1))
        self.assertEqual(schedule[3].end, pendulum.datetime(1000, 5, 1))

    def test_optimise_historic_counts_case3(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)  # Naive is 5
        self.assertEqual(min_calls, 3)  # Naive is 5
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 5, 1))

    def test_optimise_historic_counts_case4(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)  # Naive is 5
        self.assertEqual(min_calls, 7)  # Naive is 13
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.datetime(1000, 5, 1))