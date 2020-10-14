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

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import lines

from observatory.reports import AbstractObservatoryChart
from observatory.reports import chart_utils


class DistributionComparisonChart(AbstractObservatoryChart):
    """Comparison chart placing institution on distribution of others
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 plot_column: str,
                 focus_year: int,
                 world: bool = True,
                 region: bool = True,
                 country: bool = True,
                 comparison: list = None,
                 color=None):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.plot_column = plot_column
        self.world = world
        self.region = region
        self.country = country
        self.comparison = comparison
        self.color = color
        super().__init__(df)

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        self.figdata = []
        if self.world:
            world = self.df[
                self.df.published_year == self.focus_year
                ][self.plot_column].values
            self.figdata.append(world)
        if self.region:
            self.region_name = self.df[
                self.df.id == self.identifier].region.unique()[0]
            region = self.df[
                (self.df.region == self.region_name) &
                (self.df.published_year == self.focus_year)
                ][self.plot_column].values
            self.figdata.append(region)
        if self.country:
            self.country_name = self.df[
                self.df.id == self.identifier].country.unique()[0]
            country = self.df[
                (self.df.country == self.country_name) &
                (self.df.published_year == self.focus_year)
                ][self.plot_column].values
            self.figdata.append(country)

        if self.comparison is not None:
            comparison = self.df[
                (self.df.id.isin(self.comparison)) &
                (self.df.published_year == self.focus_year)
                ][self.plot_column].values
            self.figdata.append(comparison)

        self.own_value = self.df[
            (self.df.id == self.identifier) &
            (self.df.published_year == self.focus_year)
            ][self.plot_column].values[0]

    def plot(self, ax=None, ylim=None, **kwargs):
        """Plotting function
        """

        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        if 'violincolor' in kwargs:
            self.color = kwargs.get('violincolor')
        sns.violinplot(data=self.figdata,
                       ax=ax,
                       color=self.color)
        ax.set_ylim(ylim)
        ax.set_ylabel(self.plot_column)
        lineargs = {'color': 'black',
                    'linewidth': 2}
        lineargs.update(chart_utils._collect_kwargs_for(lines.Line2D, kwargs))
        ax.axhline(self.own_value, 0.05, 0.95, **lineargs)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        xlabels = []
        if not self.region:
            self.region_name = ''
        if not self.country:
            self.country_name = ''
        for label, presence in [('World', self.world),
                                (self.region_name, self.region),
                                (self.country_name, self.country),
                                ('Comparison Group', self.comparison)]:
            if presence is not None:
                xlabels.append(label)
        ax.set_xticklabels(xlabels)
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_size(12)
            label.set_ha('right')
        return self.fig
