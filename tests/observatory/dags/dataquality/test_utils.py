#!/usr/bin/python3

# Copyright 2020 Curtin University
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

import unittest
import pandas as pd
import numpy as np

from observatory.dags.dataquality.utils import proportion_delta


class TestProportionDelta(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_pandas(self):
        df1 = pd.DataFrame(data={'test': [1,2,3,4]})
        df2 = pd.DataFrame(data={'test': [2, 2, 3, 4]})
        delta = proportion_delta(df2['test'], df1['test']).to_numpy()
        np.testing.assert_array_almost_equal(delta, np.array([1,0,0,0]))

    def test_list(self):
        df1 = [1, 2, 3, 4]
        df2 = [2, 2, 3, 4]
        delta = proportion_delta(df2, df1)
        np.testing.assert_array_almost_equal(delta, np.array([1, 0, 0, 0]))

        df3 = np.array([2,2,3,4])
        delta = proportion_delta(df3, df1)
        np.testing.assert_array_almost_equal(delta, np.array([1, 0, 0, 0]))

    def test_pd_list_mix(self):
        df1 = [1, 2, 3, 4]
        df2 = pd.DataFrame([2,2,3,4])

        with self.assertRaises(ValueError):
            proportion_delta(df2, df1)

    def test_close_zero(self):
        df1 = [1, 0, 3, 4]
        df2 = [2, 0, 3, 4]
        delta = proportion_delta(df2, df1)
        np.testing.assert_array_almost_equal(delta, [1, 0, 0, 0])