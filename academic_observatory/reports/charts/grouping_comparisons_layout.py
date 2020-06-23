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

# Author: Cameron Neylon

import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc, lines
from IPython.display import HTML

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports.chart_utils import *


class GroupingComparisonsLayout(AbstractObservatoryChart):
    """Layout with three Distribution Comparison Charts for Green, Gold, Total
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 comparison: list,
                 focus_year: int,
                 country=True,
                 region=True):
        self.df = df
        self.identifier = identifier
        self.comparison = comparison
        self.focus_year = focus_year

        self.total = DistributionComparisonChart(df,
                                                 self.identifier,
                                                 'Open Access (%)',
                                                 self.focus_year,
                                                 comparison=self.comparison,
                                                 color='grey',
                                                 country=country,
                                                 region=region)
        self.gold = DistributionComparisonChart(df,
                                                self.identifier,
                                                'Total Gold OA (%)',
                                                self.focus_year,
                                                comparison=self.comparison,
                                                color='gold',
                                                country=country,
                                                region=region)
        self.green = DistributionComparisonChart(df,
                                                 self.identifier,
                                                 'Total Green OA (%)',
                                                 self.focus_year,
                                                 comparison=self.comparison,
                                                 color='darkgreen',
                                                 country=country,
                                                 region=region)

    def process_data(self):
        self.total.process_data()
        self.gold.process_data()
        self.green.process_data()

    def plot(self):
        self.fig, axes = plt.subplots(nrows=1,
                                      ncols=3,
                                      figsize=(8, 3))
        self.total.plot(ax=axes[0], ylim=(0, 100))
        self.gold.plot(ax=axes[1], ylim=(0, 100))
        self.green.plot(ax=axes[2], ylim=(0, 100))
        plt.subplots_adjust(wspace=0.5)
        return self.fig