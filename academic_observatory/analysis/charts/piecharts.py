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
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import animation, rc, artist


class OutputTypesPieChart(AbstractObservatoryChart):
    """Generate a Pie Graph based on Output Types
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int):
        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier

    def process_data(self):
        type_categories = defaults.output_types
        figdata = self.df[(self.df.id == self.identifier) &
                          (self.df.published_year == self.focus_year) &
                          (self.df.type.isin(type_categories))
                          ][['type', 'total']]
        figdata['type_category'] = pd.Categorical(
            figdata.type,
            categories=defaults.output_types,
            ordered=True)
        figdata = figdata.set_index('type_category')
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        palette = [defaults.outputs_palette[k] for k in defaults.output_types]
        outputs_pie = self.figdata.plot.pie(y='total',
                                            startangle=90,
                                            labels=None,
                                            legend=True,
                                            colors=palette,
                                            ax=ax)
        outputs_pie.set_ylabel('')
        my_circle = plt.Circle((0, 0), 0.4, color='white')
        p = plt.gcf()
        p.gca().add_artist(my_circle)
        outputs_pie.legend(labels=defaults.output_types,
                           bbox_to_anchor=(1, 0.8))
        self.fig = outputs_pie.get_figure()
        return self.fig


class CollaborationsPieChart(AbstractObservatoryChart):
    """Generate a Pie Graph based on Collaborations
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 groupcolumn: str='country',
                 countcolumn: str='count'):
        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier
        self.groupcolumn = groupcolumn
        self.countcolumn = countcolumn

    def process_data(self):
        # type_categories = defaults.output_types
        subset = self.df[(self.df.id == self.identifier) &
                          (self.df.published_year == self.focus_year)# &
                         # (self.df[self.typecolumn].isin(type_categories))
                          ][[self.groupcolumn, self.countcolumn]]

        figdata = subset.groupby([self.groupcolumn]).sum()
        self.figdata = figdata.sort_values('count', ascending=False)
        return self.figdata

    def plot(self, ax=None, **kwargs):
        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        outputs_pie = self.figdata.plot.pie(y=self.countcolumn,
                                            startangle=90,
                                            labels=None,
                                            legend=True,
                                            ax=ax)
        outputs_pie.set_ylabel('')
        my_circle = plt.Circle((0, 0), 0.4, color='white')
        p = plt.gcf()
        p.gca().add_artist(my_circle)
        handles, labels = ax.get_legend_handles_labels()
        if len(labels) > 8:
            labels=labels[0:9]
        outputs_pie.legend(labels=labels#,
                           #bbox_to_anchor=(0.8, 0.8)
                           )
        self.fig = outputs_pie.get_figure()
        return self.fig