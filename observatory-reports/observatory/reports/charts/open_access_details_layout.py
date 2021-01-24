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

from observatory.reports.charts.bar_comparison_chart import *
from observatory.reports.charts.oapc_time_chart import *


class OpenAccessDetailsLayout(AbstractObservatoryChart):
    """A Layout containing the OA levels over time and a comparison bar chart
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 comparison: list,
                 focus_year: int,
                 year_range: tuple = (2000, 2020)):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.comparison = comparison
        self.focus_year = focus_year
        self.year_range = year_range

        self.oapctimechart = OApcTimeChart(self.df,
                                           self.identifier,
                                           self.year_range)
        self.barcomparisonchart = BarComparisonChart(self.df,
                                                     self.comparison,
                                                     self.focus_year)

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        self.oapctimechart.process_data()
        self.barcomparisonchart.process_data()

    def plot(self, **kwargs):
        """Plotting function
        """

        self.fig, axes = plt.subplots(nrows=1,
                                      ncols=2,
                                      figsize=(8, 3))
        self.oapctimechart.plot(ax=axes[0])
        self.barcomparisonchart.plot(ax=axes[1])
        axes[0].set_ylabel('Open Access (% of All Outputs)')
        plt.subplots_adjust(wspace=1)
        return self.fig
