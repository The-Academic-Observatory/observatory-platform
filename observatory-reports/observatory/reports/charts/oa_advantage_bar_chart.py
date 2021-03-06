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

from observatory.reports.abstract_chart import AbstractObservatoryChart


class OAAdvantageBarChart(AbstractObservatoryChart):
    """Generates a bar chart of the OA citation advantage for a year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int):
        """Initialisation function
        """

        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier

    def process_data(self):
        """Data selection and processing function
        """

        self.df['Citations per non-OA Output'] = (self.df.total_citations -
                                                  self.df.oa_citations) / (self.df.total - self.df.oa)
        self.df['Citations per Output (all)'] = self.df.total_citations / self.df.total
        self.df['Citations per OA Output'] = self.df.oa_citations / self.df.oa
        self.df['Citations per Gold OA Output'] = self.df.gold_citations / self.df.gold
        self.df['Citations per Green OA Output'] = self.df.green_citations / self.df.green
        self.df['Citations per Hybrid OA Output'] = self.df.hybrid_citations / self.df.hybrid
        self.df['Non-Open Access'] = self.df['Citations per non-OA Output'] / self.df['Citations per Output (all)']
        self.df['Open Access'] = self.df['Citations per OA Output'] / self.df['Citations per Output (all)']
        self.df['Gold OA'] = self.df['Citations per Gold OA Output'] / self.df['Citations per Output (all)']
        self.df['Green OA'] = self.df['Citations per Green OA Output'] / self.df['Citations per Output (all)']
        self.df['Hybrid OA'] = self.df['Citations per Hybrid OA Output'] / self.df['Citations per Output (all)']
        self.columns = ['Non-Open Access',
                        'Open Access',
                        'Gold OA',
                        'Green OA',
                        'Hybrid OA']
        figdata = self.df[(self.df.id == self.identifier) &
                          (self.df.published_year == self.focus_year)
                          ][self.columns]
        figdata = figdata.melt()
        figdata.variable = pd.Categorical(figdata.variable, categories=self.columns, ordered=True)
        figdata.set_index('variable', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function
        """

        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        self.figdata.plot(kind='bar',
                          ax=ax)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_size(12)
            label.set_ha('right')
        ax.set(ylabel='Advantage (Times)', xlabel='Access Type')
        ax.axhline(1, 0, 1, color='grey', linestyle='dashed')
        ax.legend_.remove()
        return self.fig
