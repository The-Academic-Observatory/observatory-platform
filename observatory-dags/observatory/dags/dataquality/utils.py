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

import pandas as pd
import numpy as np


def proportion_delta(latest, previous):
    """ Computes proportional difference.
    @param latest: Latest data.
    @param previous: Previous data.
    @return: Proportional difference.
    """

    # If one of them is a DataFrame, we require both of them to be
    if isinstance(latest, pd.DataFrame):
        if not isinstance(previous, pd.DataFrame):
            raise ValueError('One argument is a DataFrame, but not both.')
        latest = pd.DataFrame(np.float64(latest))
        previous = pd.DataFrame(np.float64(previous))

    if isinstance(latest, list):
        latest = np.array(latest, dtype='float64')

    if isinstance(previous, list):
        previous = np.array(previous, dtype='float64')

    eps = 1e-9
    latest = latest + eps
    previous = previous + eps

    return (latest - previous) / previous
