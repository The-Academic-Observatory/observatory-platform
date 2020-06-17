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

from academic_observatory.charting import (
    AbstractObservatoryChart)
import utils.chart_utils


class GenericTimeChart(AbstractObservatoryChart):
    """Generic Graph of values over time by publication year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 columns: list,
                 identifier: str,
                 year_range: tuple = (2005, 2020)):
        self.df = df
        self.year_range = year_range
        self.columns = columns
        self.identifier = identifier
        self.melt_var_name = 'variable'

    def process_data(self):
        columns = ['id', 'Year of Publication'] + self.columns
        figdata = self.df[columns]
        figdata = self.df.melt(
            id_vars=['id',
                     'Year of Publication'],
            var_name=self.melt_var_name
        )
        figdata = figdata[
            (figdata[self.melt_var_name].isin(self.columns)) &
            (figdata.id == self.identifier) &
            (figdata['Year of Publication'].isin(range(*self.year_range)))
        ]
        figdata.value = figdata.value.astype('float64')

        figdata.sort_values('Year of Publication', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, palette=None, ax=None, lines=None, **kwargs):
        if not palette:
            palette = sns.color_palette('husl', n_colors=len(self.columns))
        if not ax:
            self.fig, ax = plt.subplots(figsize=(5, 5))
        else:
            self.fig = ax.get_figure()
        sns.lineplot(x='Year of Publication',
                     y='value',
                     data=self.figdata,
                     hue=self.melt_var_name,
                     hue_order=self.columns,
                     marker='o',
                     palette=palette,
                     ax=ax)
        ax.legend(bbox_to_anchor=(1, 0.8))
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if lines:
            ax.axhline(**lines)
        return self.fig