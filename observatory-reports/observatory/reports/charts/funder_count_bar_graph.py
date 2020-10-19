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

from observatory.reports.abstract_chart import AbstractObservatoryChart
from observatory.reports import chart_utils


class FunderCountBarGraph(AbstractObservatoryChart):
    """Single Part Funder Graph of Top 10 Funders
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 num_funders: int = 10,
                 shorten_names: int = 40
                 ):
        """Initialisation Method
        :param df: DataFrame containing data to be plotted
        :type df: pd.DataFrame
        :param identifier: The id to be selected for plotting (usually a GRID)
        :type identifier: str
        :param focus_year: The year of publication for the plot
        :type focus_year: int
        :param num_funders: Number of funders to plot, defaults to 10
        :type num_funders: int, optional
        :param shorten_names: Limit on length of the funder names to
        address an issue in matplotlib where long axis labels leads to an
        error when the figure is drawn, defaults to 30
        :type shorten_names: int, optional
        """

        self.focus_year = focus_year
        self.num_funders = num_funders
        self.identifier = identifier
        self.shorten_names = shorten_names
        super().__init__(df)

    def process_data(self):
        """Data selection and processing function
        """

        data = self.df[self.df.published_year == self.focus_year]
        if self.identifier:
            data = data[data.id == self.identifier]
        data = data.sort_values('count', ascending=False)[0:self.num_funders]

        data = data.melt(id_vars=['published_year', 'name'],
                         var_name='variables')
        # Shorten the funders names to avoid an issue that can arise where
        # the length of the axis labels leads to a matplotlib error
        # when the figure is drawn ValueError: left cannot be >= right
        data.loc[:, 'name'] = data['name'].apply(
            lambda s: s[0:self.shorten_names])
        self.figdata = data
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function
        """

        fig_kwargs = chart_utils._collect_kwargs_for(plt.figure, kwargs)
        if not ax:
            self.fig, ax = plt.subplots(**fig_kwargs)
        else:
            self.fig = ax.get_figure()

        sns.barplot(y="name",
                    x="value",
                    hue="variables",
                    data=self.figdata[self.figdata.variables.isin(['count'])],
                    ax=ax)
        ax.set(ylabel=None, xlabel='Number of Outputs')

        return self.fig
