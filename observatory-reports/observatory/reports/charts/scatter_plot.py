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

import matplotlib.axis
import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import HTML
from matplotlib import animation
import plotly.express as px
from typing import Union, List, Tuple
import seaborn as sns

from observatory.reports.abstract_chart import AbstractObservatoryChart
from observatory.reports.chart_utils import _collect_kwargs_for
from observatory.reports import defaults


class ScatterPlot(AbstractObservatoryChart):
    """
    Scatterplot based on sns.scatterplot for COKI data

    Generates a standard scatter plot with default colors based
    on the region color palette and size of points based on the
    total outputs of the university

    :param AbstractObservatoryChart: [description]
    :type AbstractObservatoryChart: [type]
    :return: [description]
    :rtype: [type]
    """

    def __init__(self,
                 df: pd.DataFrame,
                 x: str,
                 y: str,
                 filter_name: str,
                 filter_value: Union[str, int, float, List[int], Tuple[int, int]],
                 hue_column: str = 'region',
                 size_column: str = 'total_outputs',
                 focus_id: str = None,
                 focus_marker: str = 'x',
                 focus_marker_color: str = 'black',
                 **kwargs):
        """Initialisation Method

        :param df: DataFrame with data to plot
        :type df: pd.DataFrame
        :param x: Name of the column containing x-data
        :type x: str
        :param y: Name of the column containing y-data
        :type y: str
        :param filter_name: Name of the column to filter data on
        :type filter_name: str
        :param filter_value: Value of column to filter with. If a value
        (str, int etc) will be compared to values in the `filter_name` column.
        A 2-tuple will be expanded to a range (which assumes the components are
        ints representing years)
        :type filter_value: list, 2-tuple of ints, or value
        :param hue_column: Name of the column to define the color
        of plotted points, defaults to 'region'
        :type hue_column: str, optional
        :param size_column: Name of the column to use to define the size
        of the plotted points, defaults to 'total'
        :type size_column: str, optional
        :param focus_id: Identifier for an organisation to emphasise on
        the plot by plotting a black cross, defaults to None
        :type focus_id: str, optional
        """

        super().__init__(df)
        self.x = x
        self.y = y
        self.filter_name = filter_name

        if ((type(filter_value) == tuple) or (type(filter_value) == list)) \
                and (type(filter_value[0]) == int) \
                and (len(filter_value) == 2):
            self.filter_value = range(*filter_value)
        elif type(filter_value) != list:
            self.filter_value = [filter_value]
        else:
            self.filter_value = filter_value

        self.hue_column = hue_column
        self.size_column = size_column
        self.focus_id = focus_id
        self.focus_marker = focus_marker
        self.focus_marker_color = focus_marker_color
        self.processed = False
        self.kwargs = kwargs

    def process_data(self) -> pd.DataFrame:
        """Data processing function

        Filter the data. If hue_column is set to region then sort based on region and
        set an order that works reasonably well for the OA plots.
        """

        figdata = self.df
        figdata = figdata[figdata[self.filter_name].isin(self.filter_value)]
        if self.hue_column == 'region':
            self.sorter = ['Asia', 'Europe', 'North America',
                      'Latin America', 'Africa', 'Oceania']
            additional_legend_items = set(figdata[self.hue_column].unique()) - set(self.sorter)
            self.sorter.extend(list(additional_legend_items))
            sorter_index = dict(zip(self.sorter, range(len(self.sorter))))
            figdata.loc[:, 'order'] = figdata.region.map(sorter_index)
            figdata = figdata.sort_values('order', ascending=True)
        else:
            self.sorter = None
        self.figdata = figdata
        self.processed = True
        return self.figdata

    def plotly(self,
               colorpalette: Union[list, dict] = None,
               **kwargs):
        if not self.processed:
            self.process_data()

        # Organise the palette options
        if type(colorpalette) == list:
            color_discrete_sequence = colorpalette
            color_discrete_map = None
        elif type(colorpalette) == dict:
            color_discrete_sequence = None
            color_discrete_map = colorpalette
        elif not colorpalette:
            color_discrete_sequence = None
            color_discrete_map = defaults.region_palette

        # Detect whether a year range is set or single year to set up an animation
        animation_frame = None
        animation_group = None
        if self.filter_name in ['Year', 'Year of Publication', 'published_year', 'time_period']:
            self.figdata.sort_values(self.filter_name, ascending=True, inplace=True)
            if len(self.filter_value) > 1:
                animation_frame = self.filter_name
                animation_group = 'id'

        if self.focus_id:
            name = self.figdata.loc[(self.figdata.id == self.focus_id), 'name'].values[0]
            self.figdata.loc[(self.figdata.id == self.focus_id), self.hue_column] = name
            if color_discrete_map:
                color_discrete_map.update({name:'black'})
        scatter_kwargs = _collect_kwargs_for(px.scatter, kwargs)
        fig = px.scatter(self.figdata,
                         x=self.x,
                         y=self.y,
                         size=self.size_column,
                         color=self.hue_column,
                         color_discrete_sequence=color_discrete_sequence,
                         color_discrete_map=color_discrete_map,
                         category_orders=dict(region=self.sorter),
                         animation_frame=animation_frame,
                         animation_group=animation_group,
                         hover_name='name',
                         range_x=[0,100],
                         range_y=[0,100],
                         **scatter_kwargs)
        return fig

    def plot(self,
             ax: matplotlib.axis = None,
             colorpalette: sns.color_palette = None,
             additional_filter=None,
             **kwargs) -> matplotlib.figure:
        """Plot method for scatter plots

        :param ax: The matplotlib axis to plot to. If None creates a new
        figure, defaults to None
        :type ax: matplotlib.axis, optional
        :param colorpalette: A seaborn or matplotlib color palette used to
        set the colors for the plot. If the default None is passed, the
        regioncolorpalette is used, defaults to None
        :type colorpalette: sns.color_palette, optional
        :param additional_filter: <tuple> with exactly two elements, one
        being the column to filter on and the other the value.
        Used for animations, defaults to None
        :type additional_filter: tuple, optional
        :param kwargs: Keyword arguments for sns.relplot, matplotlib
        plt.subplots and matplotlib ax.set should be picked up and
        distributed to the appropriate functions.
        :return: The rendered matplotlib figure is returned and also
        available at self.fig
        :rtype: matplotlib.figure
        """

        scatterplot_kwargs = _collect_kwargs_for(sns.scatterplot, kwargs)
        fig_kwargs = _collect_kwargs_for(plt.figure, kwargs)
        if not ax:
            self.fig, self.ax = plt.subplots(**fig_kwargs)
        else:
            self.ax = ax
            self.fig = ax.get_figure()

        if not colorpalette:
            colorpalette = defaults.region_palette

        figdata = self.figdata
        if additional_filter:
            col, value = additional_filter
            figdata = figdata[figdata[col] == value]

        sns.scatterplot(x=self.x, y=self.y, data=figdata,
                        size=self.size_column, sizes=(50, 600),
                        hue=self.hue_column, alpha=0.6,
                        palette=colorpalette,
                        ax=self.ax,
                        **scatterplot_kwargs)
        if self.focus_id:
            sns.scatterplot(x=self.x, y=self.y,
                            data=figdata[figdata.id == self.focus_id],
                            color=self.focus_marker_color, s=200, marker=self.focus_marker, legend=False,
                            ax=self.ax)
        self.ax.spines['top'].set_visible(False)
        self.ax.spines['right'].set_visible(False)
        self.ax.legend(bbox_to_anchor=(1.1, 0.8))
        self.ax.set(**kwargs)
        return self.fig

    def animate(self,
                colorpalette: sns.color_palette = None,
                year_range=None,
                numframes: int = None,
                frameinterval: int = 1000,
                **kwargs):
        """Generate an animated scatter plot

        :param colorpalette: Searborn or matplotlib color palette for the
        scatter plot, defaults to None
        :type colorpalette: sns.color_palette, optional
        :param year_range: Optional parameter, with the default None it will
        use self.filter_value as set when the object was initialised. A tuple
        with two or three elements that will passed to range to generate a list
        of years or a list of years that will be used directly,
        defaults to None
        :type year_range: tuple or list, optional
        :param numframes: Optional set of frames to animate, with the default
        None the number of frames will be set to the number of years plus five
        to create a pause at the end of the animation, defaults to None
        :type numframes: int, optional
        :param frameinterval: Optional to set the frame rate of the animation
        in milliseconds. Defaults to one frame per second i.e 1000
        :type frameinterval: int, optional
        :return: HTML5 video representation of the animation
        :rtype:
        """

        fig_kwargs = _collect_kwargs_for(plt.figure, kwargs)
        self.plot_kwargs = kwargs
        self.color_palette = colorpalette

        if year_range:
            if type(year_range) == tuple:
                self.year_range = range(*year_range)
            elif type(year_range) == list:
                self.year_range == year_range
        elif not year_range:
            self.year_range = self.filter_value

        if not numframes:
            numframes = len(self.year_range) + 5

        self.fig, self.ax = plt.subplots(**fig_kwargs)

        self.anim = animation.FuncAnimation(self.fig,
                                            self.anim_frame,
                                            numframes,
                                            interval=frameinterval)
        return HTML(self.anim.to_html5_video())

    def anim_frame(self, i: int):
        """Frame animation function for scatterplot

        :param i: framenumber
        :return: None
        """

        year = self.year_range[0] + i + 1
        if year in self.year_range:
            yearstring = str(year)
        else:
            year = self.year_range[-1]
            yearstring = str(year)

        self.ax.clear()
        self.plot(ax=self.ax,
                  colorpalette=self.color_palette,
                  additional_filter=('published_year', year),
                  **self.plot_kwargs)

        self.ax.text(0.05, 0.95,
                     yearstring,
                     transform=self.ax.transAxes,
                     fontsize=14,
                     verticalalignment='top')
        plt.close()
