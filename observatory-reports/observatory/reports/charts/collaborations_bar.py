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

from observatory.reports import AbstractObservatoryChart


class CollaborationsBar(AbstractObservatoryChart):
    """Generates a bar chart of Collaborations
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 own_count: int,
                 type: str = 'percent',
                 number: int = 10):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.own_count = own_count
        self.type = type
        self.number = number

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        figdata = self.df[(self.df.published_year == self.focus_year) &
                          (self.df.id == self.identifier)
                          ].sort_values('count', ascending=False)[['name', 'count']][0:self.number]
        if self.type != 'percent':
            figdata = figdata.append(pd.DataFrame.from_dict({'count': self.own_count, 'name': 'Total Outputs'}))
        else:
            figdata['count'] = figdata['count'].apply(lambda c: c / self.own_count * 100)
        figdata = figdata.set_index('name')
        figdata.sort_values('count', ascending=False, inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function
        """

        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        self.figdata.plot(
            kind='bar', ax=ax)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for label in ax.get_xticklabels():
            label.set_rotation(45)
            label.set_ha('right')
        ax.get_legend().set_visible(False)
        if self.type == 'percent':
            ax.set(ylabel='Collaborations (% of outputs)', xlabel=None)
        else:
            ax.set(ylabel='Count of collaborative outputs', xlabel=None)
        return self.fig
