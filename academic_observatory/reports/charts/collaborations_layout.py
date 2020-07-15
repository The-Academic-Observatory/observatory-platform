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
import matplotlib.gridspec as gs

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports import chart_utils
from academic_observatory.reports.charts.coordinates_map import *
from academic_observatory.reports.charts.collaborations_bar import *
from academic_observatory.reports.charts.chloropleth_map import *


class CollaborationsLayout(AbstractObservatoryChart):
    """Generates the layout for Collaborations plots
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 own_count,
                 xlim: tuple = (-170, 180),
                 ylim: tuple = (-60, 85),
                 maptype='coordinates'):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.own_count = own_count
        self.xlim = xlim
        self.ylim = ylim

        if maptype == 'coordinates':
            self.map = CoordinatesMap(df, identifier, focus_year, xlim, ylim)
        elif maptype == 'countries':
            self.map = ChloroplethMap(df, identifier, focus_year,
                                      geocolumn='collab_id',
                                      xlim=xlim,
                                      ylim=ylim)
        self.bar = CollaborationsBar(df, identifier, focus_year, own_count, number=7)

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        self.map.process_data()
        self.bar.process_data()

    def plot(self, **kwargs):
        """Plotting function
        """

        self.fig = plt.Figure(figsize=(8, 3))
        grid = gs.GridSpec(3, 2,
                           width_ratios=[2, 1],
                           height_ratios=[1, 2, 1],
                           wspace=0.3)
        axes = [self.fig.add_subplot(grid[:, :-1]), self.fig.add_subplot(grid[1:2, 1])]

        self.map.plot(ax=axes[0])
        self.bar.plot(ax=axes[1])
        return self.fig
