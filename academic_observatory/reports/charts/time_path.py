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
from matplotlib import animation, rc, artist
from IPython.display import HTML
from abc import ABC, abstractmethod

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports.chart_utils import *


class TimePath(AbstractObservatoryChart):
    """Charts to illustrate movement over time in two dimensions
    """

    def __init__(self,
                 df: pd.DataFrame,
                 year_range: tuple,
                 unis: list,
                 x: str,
                 y: str,
                 hue_column: str = 'name',
                 size_column: str = None,
                 **kwargs):
        """
        param: df: input data frame
        param: year_range: duple containing first and last+1 year
        param: unis: a list of ids
        param: x: str, column name for x values
        param: y: str, column name for y values
        """

        self.xcolumn = x
        self.ycolumn = y
        self.year_range = range(*year_range)
        self.unis = unis
        self.hue_column = hue_column
        self.size_column = size_column
        super().__init__(df)

    def process_data(self, **kwargs):
        figdata = self.df
        for uni in self.unis:
            try:
                if 'grid_id' in figdata.columns:
                    assert uni in figdata['grid_id'].values
                if 'id' in figdata.columns:
                    assert uni in figdata['id'].values
            except AssertionError:
                print(uni, 'not in list of ids')
        figdata = figdata[(figdata.id.isin(self.unis)) &
                          figdata.published_year.isin(self.year_range)]
        figdata['order'] = figdata['id'].map(
            lambda v: self.unis.index(v))
        figdata = figdata.sort_values(
            ['order', 'published_year'], ascending=True)
        self.df = figdata
        return self.df

    def plot(self, year_range=None, colorpalette=None, ax=None, **kwargs):
        if not year_range:
            year_range = self.year_range
        if not colorpalette:
            colorpalette = sns.color_palette("husl", len(self.unis))

        if not ax:
            figsize = kwargs.pop('figsize', None)
            self.fig, ax = plt.subplots(figsize=figsize)

        else:
            self.fig = ax.get_figure()

        figdata = self.df[self.df.published_year.isin(year_range)]

        sns.scatterplot(x=self.xcolumn, y=self.ycolumn,
                        data=figdata, s=20,
                        hue=self.hue_column, ax=ax, palette=colorpalette)
        sns.lineplot(x=self.xcolumn, y=self.ycolumn,
                     data=figdata, sort=False, legend=False,
                     hue=self.hue_column, ax=ax, palette=colorpalette)

        head_width = kwargs.pop('arrow_width', None)
        if not head_width:
            head_width = 2

        if len(year_range) > 1:
            for i, uni in enumerate(self.unis):
                x = figdata[
                    (figdata.id == uni) &
                    (figdata.published_year == year_range[-2])
                ][self.xcolumn].iloc[0]
                y = figdata[
                    (figdata.id == uni) &
                    (figdata.published_year == year_range[-2])
                ][self.ycolumn].iloc[0]
                dx = figdata[
                    (figdata.id == uni) &
                    (figdata.published_year == year_range[-1])
                ][self.xcolumn].iloc[0] - x
                dy = figdata[
                    (figdata.id == uni) &
                    (figdata.published_year == year_range[-1])
                ][self.ycolumn].iloc[0] - y
                try:
                    color = colorpalette[i]
                except TypeError:
                    _, color = colorpalette.items()[i]
                ax.arrow(x, y, dx, dy, color=color, head_width=head_width)

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.legend(bbox_to_anchor=(1.1, 0.8))
        ax.set(**kwargs)
        return self.fig

    def animate(self, colorpalette=None, year_range=None, **kwargs):
        self.plot_kwargs = kwargs
        self.color_palette = colorpalette
        if not year_range:
            year_range = self.year_range

        figsize = kwargs.pop('figsize', None)
        fig, self.ax = plt.subplots(figsize=figsize)

        self.anim = animation.FuncAnimation(fig, self.anim_frame,
                                            (len(year_range)+5), interval=1000)

        return HTML(self.anim.to_html5_video())

    def anim_frame(self, i):
        self.ax.clear()
        self.plot(self.year_range[0:i+2], colorpalette=self.color_palette,
                  ax=self.ax, **self.plot_kwargs)
        year = self.year_range[0] + i + 1
        if year in self.year_range:
            yearstring = f'{self.year_range[0]} - {year}'
        else:
            yearstring = f'{self.year_range[0]} - {self.year_range[-1]}'
        self.ax.text(0.05, 0.95,
                     yearstring,
                     transform=self.ax.transAxes,
                     fontsize=14,
                     verticalalignment='top')
        plt.close()