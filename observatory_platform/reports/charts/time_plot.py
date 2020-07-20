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

import itertools

import matplotlib.pyplot as plt
import pandas as pd

from observatory_platform.reports import AbstractObservatoryChart
from observatory_platform.reports.chart_utils import _collect_kwargs_for


class TimePlot(AbstractObservatoryChart):
    """Line charts for showing points of change in time
    """

    def __init__(self,
                 df: pd.DataFrame,
                 year_range: tuple,
                 unis: list,
                 plot_column: str,
                 hue_column: str = 'name',
                 size_column: str = None,
                 **kwargs):
        """Initialisation function

        param: year_range: tuple with two elements for range of years to plot
        param: unis: list of grid_ids to include
        param: plot_column: name of column of input df to use as values
        return: None
        """

        self.year_range = range(*year_range)
        self.unis = unis
        self.plot_column = plot_column
        self.hue_column = hue_column
        self.size_column = size_column
        self.kwargs = kwargs
        super().__init__(df)

    def process_data(self, *kwargs):
        """Data selection and processing function
        """

        figdata = self.df
        columnorder = [figdata[figdata.id == grid].iloc[0]['name']
                       for grid in self.unis]
        figdata = figdata[(figdata.published_year.isin(
            self.year_range)) & (figdata.id.isin(self.unis))]
        figdata = figdata.pivot(index='published_year',
                                columns="name", values=self.plot_column)
        figdata = figdata.reindex(columnorder, axis=1)
        self.df = figdata
        return self.df

    def plot(self, ax=None, xticks=None, marker_line=None,
             ylim=None, **kwargs):
        """Plotting function
        """

        plot_kwargs = _collect_kwargs_for(plt.subplots, kwargs)
        if not ax:
            self.fig, axes = plt.subplots(len(self.unis), 1, sharex=True,
                                          frameon=False, **plot_kwargs)
            self.df.plot(subplots=True, ax=axes, legend=False,
                         color='black', title=[n for n in self.df.columns])

        else:
            axes = self.df.plot(subplots=True, ax=ax, legend=False,
                                color='black',
                                title=[n for n in self.df.columns])

        [ax.spines[loc].set_visible(False) for ax, loc in itertools.product(
            axes, ['top', 'right', 'bottom'])]
        [ax.tick_params(axis='x', which='both', bottom=False, top=False,
                        labelbottom=False) for ax in axes[0:len(self.unis) - 1]]
        if ylim:
            if len(ylim) == 2:
                b, t = ylim
                [ax.set_ylim(bottom=b, top=t) for ax in axes[0:len(self.unis)]]
            else:
                [ax.set_ylim(bottom=ylim) for ax in axes[0:len(self.unis)]]
        [ax.title.set_ha('left') for ax in axes[0:len(self.unis)]]
        [ax.title.set_position([0.03, 0.5]) for ax in axes[0:len(self.unis)]]

        axes[-1].spines['bottom'].set_visible(True)
        if xticks:
            axes[-1].set_xticks(xticks)
        axes[-1].tick_params(axis='x', which='minor', bottom=False)

        if marker_line:
            [ax.axvline(marker_line, 0, 1.2, color='grey',
                        linestyle='dashed', clip_on=False) for ax in axes]

        return self.fig
