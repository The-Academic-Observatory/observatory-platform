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

from observatory.reports import AbstractObservatoryChart
from observatory.reports import chart_utils


class ComparisonTimeBar(AbstractObservatoryChart):
    """Generate a Time Bar plot based on Comparisons
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple,
                 x: str = 'Year of Publication',
                 y: str = 'Total Publications',
                 hue: str = 'name',
                 comparison: list = [],
                 linepalette: sns.color_palette = None,
                 barpalette: sns.color_palette = ['brown', 'orange', 'gold', 'green']):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.year_range = year_range
        self.x = x
        self.y = y
        self.hue = hue
        self.comparison = comparison
        self.linepalette = linepalette
        self.barpalette = barpalette

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        if self.identifier not in self.comparison:
            comparison = [self.identifier] + self.comparison
        else:
            comparison = self.comparison
        figdata = self.df[(self.df.published_year.isin(range(*self.year_range))) &
                          (self.df.id.isin(comparison))
                          ]

        figdata['Comparisons'] = figdata[self.hue]

        figdata.sort_values(self.x, ascending=True, inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function
        """

        name = chart_utils.id2name(self.df, self.identifier)
        if not ax:
            fig_kwargs = chart_utils._collect_kwargs_for(plt.figure, kwargs)
            self.fig, ax = plt.subplots(**fig_kwargs)
        else:
            self.fig = ax.get_figure()

        if self.comparison:
            color = 'darkgrey'
        else:
            color = 'steelblue'

        if type(self.y) == str:
            sns.barplot(x=self.x, y=self.y,
                        ax=ax, color=color,
                        data=self.figdata[self.figdata.id == self.identifier])

        elif type(self.y) == list:
            bardata = self.figdata[self.figdata.id == self.identifier]
            bardata.set_index(self.x, inplace=True)
            bardata[self.y].plot(kind='bar',
                                 stacked=True,
                                 colors=self.barpalette,
                                 ax=ax)
            ax.legend(bbox_to_anchor=(1.1, 0.8))

        else:
            raise TypeError('y must be a column name or list of column names')

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        for label in ax.get_xticklabels():
            label.set_rotation(45)
            label.set_ha('right')

        if self.comparison:
            ax2 = ax.twiny()
            linedata = self.figdata[self.figdata['Comparisons'] != name]
            lastyear = linedata[self.x].max()
            order = list(linedata[linedata[self.x] == lastyear].sort_values(
                self.y,
                ascending=False)[self.hue])

            if not self.linepalette:
                self.linepalette = sns.color_palette('husl', n_colors=10)
            if self.identifier in self.comparison:
                self.linepalette = ['grey'] + self.linepalette
                linedata = self.figdata
                order = [name] + order
            self.linepalette = self.linepalette[0:len(order)]
            sns.lineplot(x=self.x, y=self.y,
                         hue='Comparisons', hue_order=order, palette=self.linepalette,
                         ax=ax2,
                         data=linedata)

            ax2.spines['top'].set_visible(False)
            ax2.spines['right'].set_visible(False)
            ax2.get_xaxis().set_visible(False)
            ax2.get_yaxis().set_visible(False)
            ax2.legend(bbox_to_anchor=(1.1, 0.8))

        return self.fig
