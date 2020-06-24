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

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports import chart_utils


class ChloroplethMap(AbstractObservatoryChart):
    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 geocolumn: str='collab_id',
                 countcolumn: str='count',
                 xlim: tuple = (-170, 180),
                 ylim: tuple = (-60, 85)):
        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.geocolumn = geocolumn
        self.countcolumn = countcolumn
        self.xlim = xlim
        self.ylim = ylim

    def process_data(self):
        filtered = self.df[(self.df.published_year == self.focus_year) &
                          (self.df.id == self.identifier)]
        filtered.set_index(self.geocolumn, inplace=True)
        self.world = geopandas.read_file(
            geopandas.datasets.get_path('naturalearth_lowres')
        )
        self.figdata = self.world.join(filtered[self.countcolumn], on='iso_a3')
        self.logcounts = np.log(self.figdata[self.countcolumn])

    def plot(self, ax=None, **kwargs):
        if not ax:
            self.fig, ax = plt.subplots()
        else:
            self.fig = ax.get_figure()
        self.figdata.plot(column=self.logcounts, ax=ax,
                          edgecolor='grey')
        ax.set_axis_off()
        if self.xlim:
            ax.set(xlim=self.xlim)
        if self.ylim:
            ax.set(ylim=self.ylim)
        return self.fig