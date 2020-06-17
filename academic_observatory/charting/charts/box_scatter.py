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
from matplotlib import animation, rc
from IPython.display import HTML

from academic_observatory.charting.charts import AbstractObservatoryChart
from utils.chart_utils import region_palette, _collect_kwargs_for


class BoxScatter(AbstractObservatoryChart):
    """
    Box and scatter charts for groupings of universitiies
    """

    def __init__(self,
                 df: pd.DataFrame,
                 year: int,
                 group_column: str,
                 plot_column: str,
                 sort: bool = True,
                 sortfunc: callable = pd.DataFrame.median,
                 **kwargs):
        """

        param: df: pd.DataFrame containing data to be plotted
        param: year: int for the year to be plotted
        param: group_column: str giving the column to group by for plot
        param: plot_column: str giving the column to use as values to plot
                            or
                            list giving the set of columns to use
        param: sort: bool default True, whether to sort the groups
        param: sortfunc: pd.DataFrame function, default median
        """

        self.year = year
        self.group_column = group_column
        self.plot_column = plot_column
        self.sort = sort
        self.sortfunc = sortfunc
        super().__init__(df)

    def process_data(self, *kwargs):

        figdata = self.df
        figdata = figdata[figdata.published_year == self.year]
        grouped = figdata.groupby(self.group_column)
        if type(self.plot_column) == list:
            plot_column = self.plot_column[0]
        else:
            plot_column = self.plot_column
            self.plot_column = [self.plot_column]

        if self.sort:
            sort_df = pd.DataFrame(
                {col: vals[plot_column] for col, vals in grouped})
            sfunc = self.sortfunc
            self.group_order = sfunc(sort_df).sort_values(ascending=False)

        self.df = figdata
        return self.df

    def plot(self,
             color: str = 'silver',
             figsize: tuple = (15, 20),
             dodge: bool = False,
             hue: str = 'region',
             xticks: list = [0, 20, 40, 60, 80],
             colorpalette: sns.color_palette = region_palette,
             alpha: float = 0.5,
             **kwargs):
        """Plot Method

        param: color color for the box plots, must be a color name
                     for matplotlib
        param: figsize tuple with two elements to pass to figsize
        param: dodge sns.boxplot keyword argument for points to avoid
                     overlap
        """

        fig, axes = plt.subplots(1, len(self.plot_column),
                                 figsize=figsize,
                                 sharey=True, sharex=True,
                                 frameon=False)

        if self.sort:
            order = self.group_order.index
        else:
            order = self.df.index
        if len(self.plot_column) > 1:
            panels = zip(axes, self.plot_column)
        else:
            panels = [(axes, self.plot_column[0])]

        for ax, plot_column in panels:
            sns.boxplot(x=plot_column, y=self.group_column,
                        data=self.df, ax=ax,
                        order=order, color="silver", dodge=False)
            sns.swarmplot(x=plot_column, y=self.group_column,
                          data=self.df, ax=ax,
                          order=order, hue='region',
                          palette=colorpalette, alpha=alpha)
            ax.yaxis.label.set_visible(False)
            if xticks:
                ax.set_xticks(xticks)
            ax.get_legend().remove()
        fig.subplots_adjust(wspace=0)
        return fig