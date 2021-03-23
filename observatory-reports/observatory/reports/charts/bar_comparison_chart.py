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
import plotly.express as px
from typing import Union

from observatory.reports.abstract_chart import AbstractObservatoryChart


class BarComparisonChart(AbstractObservatoryChart):
    """Generates BarPlot of OA outputs with helpful defaults

    Produces a standard bar plot with appropriate colors for
    Bronze, Hybrid, DOAJ_Gold and Green OA.
    """

    def __init__(self,
                 df: pd.DataFrame,
                 comparison: list,
                 focus_year: int,
                 color_palette=['brown', 'orange', 'gold', 'green']
                 ):
        """Initialisation function

        param: df: pd.DataFrame in the standard COKI format
        param: comparison: <list> of grid IDs
        param: year: <int> year for the data to be used to plot
        param: color_palette: matplotlib color palette, default colors
                              used for an OA types contribution bar plot
        """

        self.comparison = comparison

        if ((type(focus_year) == tuple) or (type(focus_year) == list)) \
                and (type(focus_year[0]) == int) \
                and (len(focus_year) == 2):
            self.focus_year = range(*focus_year)
        elif type(focus_year) != list:
            self.focus_year = [focus_year]
        else:
            self.focus_year = focus_year

        self.color_palette = color_palette
        self.df = df
        self.processed = False

    def process_data(self, **kwargs):
        """Data selection and processing function

        param: kwargs: Keyword arguments, currently unused
        """

        figdata = self.df[(self.df.published_year.isin(self.focus_year)) &
                          (self.df.id.isin(self.comparison))]
        if len(self.focus_year) ==1:
            figdata = figdata.set_index('id').reindex(self.comparison)
            figdata['Comparators'] = figdata.name
            figdata = figdata.set_index('name')
        else:
            figdata['order'] = figdata.id.map({id:order for order, id in enumerate(self.comparison)})
            figdata.sort_values(['order', 'Year of Publication'], ascending=True, inplace=True)
        self.figdata = figdata
        self.processed = True
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function

        param: kwargs: Any keywords to be sent to plt.figure or ax.set
                       during the plotting process.
        """

        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        self.figdata[['Bronze (%)',
                      'Hybrid OA (%)',
                      'Gold in DOAJ (%)',
                      'Green Only (%)']].plot(
            kind='bar', stacked=True, colors=self.color_palette, ax=ax)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha('right')

        ax.set(ylabel='Percent of all outputs', xlabel=None)
        ax.legend(bbox_to_anchor=(1, 0.8))
        return self.fig


    def plotly(self,
               **kwargs):

        if not self.processed:
            self.process_data()

        # Detect whether a year range is set or single year to set up an animation
        animation_frame = None
        animation_group = None
        if len(self.focus_year) > 1:
            animation_frame = 'Year of Publication'
            animation_group = 'id'

        fig = px.bar(self.figdata,
                     x='name',
                     y=['Bronze (%)',
                      'Hybrid OA (%)',
                      'Gold in DOAJ (%)',
                      'Green Only (%)'],
                     color_discrete_sequence=self.color_palette,
                     animation_frame=animation_frame,
                     animation_group=animation_group,
                     range_y=(0,100),
                     labels=dict(variable='Open Access Category',
                                 name=''))
        fig.update_yaxes(title='Open Access (%)')
        fig.update_xaxes(tickangle=-30)
        self.fig = fig
        return fig