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
from academic_observatory.reports.chart_utils import *

class OACitationsLayout(AbstractObservatoryChart):
    """Layout with Citations by OA class and OA Advantage Bar Chart"""

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int,
                 year_range: tuple = (2001, 2019)):
        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.year_range = year_range

        self.citationsovertime = CitationCountTimeChart(self.df,
                                                        self.identifier,
                                                        self.year_range,
                                                        chart_type='per-article')
        self.oaadvantagebar = OAAdvantageBarChart(self.df,
                                                  self.identifier,
                                                  self.focus_year)

    def process_data(self):
        self.citationsovertime.process_data()
        self.oaadvantagebar.process_data()

    def plot(self, **kwargs):
        self.fig, axes = plt.subplots(nrows=1,
                                      ncols=2,
                                      figsize=(8, 3))
        self.citationsovertime.plot(ax=axes[0])
        self.oaadvantagebar.plot(ax=axes[1])
        axes[0].set_ylabel('Total Citations per Output (all time)')
        plt.subplots_adjust(wspace=1)
        return self.fig