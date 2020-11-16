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

from observatory.reports.charts.output_types_pie_chart import *
from observatory.reports.charts.output_types_time_chart import *


class OutputTypesLayout(AbstractObservatoryChart):
    """A Layout containing graph of output types over time and pie chart
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 year_range: tuple = (2000, 2020)):
        """Initialisation function
        """

        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier
        self.year_range = year_range

        self.outputstimechart = OutputTypesTimeChart(self.df,
                                                     self.identifier,
                                                     self.year_range)
        self.outputspiechart = OutputTypesPieChart(self.df,
                                                   self.identifier,
                                                   self.focus_year)

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        self.outputstimechart.process_data()
        self.outputspiechart.process_data()

    def plot(self, **kwargs):
        """Plotting function
        """

        self.fig, axes = plt.subplots(nrows=1,
                                      ncols=2,
                                      figsize=(8, 3))
        self.outputstimechart.plot(ax=axes[0])
        self.outputspiechart.plot(ax=axes[1])

        legend = axes[0].get_legend()
        legend.remove()
        axes[0].set_ylabel('Number of Outputs')
        return self.fig
