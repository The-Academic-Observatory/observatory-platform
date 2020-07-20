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
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports import defaults


class ConfidenceIntervalRank(AbstractObservatoryChart):
    """Ranking chart with confidence intervals plotted
    """

    def __init__(self,
                 df: pd.DataFrame,
                 rankcol: str,
                 errorcol: str,
                 filter_name: str,
                 filter_value,
                 rank_length: int = 100,
                 valcol: str = None,
                 colordict: dict = None):
        """Initialisation Function

        param: df: pd.DataFrame in the standard COKI format
        param: rankcol: <str> with the name of column containing values to
                        base the ranking on
        param: errorcol: <str> with the name of column containing values for
                         the length of the error bars
        param: filter_name: <str> with the name of the column to use for
                            filtering the data, generally a year
        param: filter_value: <str> or <int> value to use for filtering the
                             data to display. Generally the year as an <int>
        param: rank_length: <int> Length of the ranking to compute and plot
        param: valcol: <str> with name of column to use for the values to be
                       plotted against the rank (if different to rankcol)
        param: colordict: <dict> to convert a column to colors for the lines
        """

        super().__init__(df)
        self.rankcol = rankcol
        self.valcol = valcol
        self.errorcol = errorcol
        self.filter_name = filter_name
        self.filter_value = filter_value
        self.colordict = colordict
        self.rank_length = rank_length

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        figdata = self.df
        figdata = figdata[figdata[self.filter_name] == self.filter_value]
        if not self.valcol:
            self.valcol = self.rankcol

        figdata = figdata.sort_values(self.rankcol,
                                      ascending=False)[0:self.rank_length]
        figdata['Rank'] = figdata[self.rankcol].rank(ascending=False)

        if 'region' in figdata.columns:
            figdata['color'] = figdata['region'].map(defaults.region_palette)
        else:
            figdata['color'] = 'grey'
        self.df = figdata
        return self.df

    def plot(self,
             ax: matplotlib.axis = None,
             show_rank_axis: bool = True,
             **kwargs):
        """Plotting function
        """

        if ax is None:
            yaxis = plt.gca()
        else:
            yaxis = ax

        with sns.plotting_context('paper',
                                  rc={'legend.fontsize': 10,
                                      'axes.labelsize': 10},
                                  font_scale=1):
            yaxis.invert_yaxis()
            yaxis.errorbar(self.df[self.rankcol], self.df['name'],
                           xerr=self.df[self.errorcol],
                           fmt='o', ecolor=self.df['color'], elinewidth=4,
                           marker='|', mfc='black', mec='black', ms=8, mew=1.5)
            yaxis.spines['top'].set_visible(False)
            yaxis.spines['right'].set_visible(False)
            yaxis.set_xlabel(f'{self.rankcol}')
