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

from utils.chart_utils import _collect_kwargs_for, id2name, region_palette


class Layout(AbstractObservatoryChart):
    """General Class for handling multi-chart layouts


    """

    def __init__(self, df, charts):
        """
        :param df: A data frame conforming to the COKI table format
        :param charts: A list of dictionaries containing the
               initiatialisation params and kwargs for the sub-charts
        :return: A figure with the relevant charts as subplots
        """

        self.chart_params = charts
        self.charts = []
        super().__init__(df)

    def process_data(self):
        for params in self.chart_params:
            params['df'] = self.df
            chart_class = params.pop('chart_class')
            chart = chart_class(**params)
            self.charts.append(chart)

        for chart in self.charts:
            chart.process_data()

    def plot(self,
             figsize=(15, 20),
             **kwargs):
        fig, axes = plt.subplots(1, len(self.charts),
                                 figsize=figsize,
                                 sharey=False,
                                 sharex=False,
                                 frameon=False)

        for chart, ax in zip(self.charts, axes):
            chart.plot(ax=ax, **kwargs)

        if 'wspace' in kwargs:
            fig.subplots_adjust(wspace=kwargs['wspace'])

        return fig