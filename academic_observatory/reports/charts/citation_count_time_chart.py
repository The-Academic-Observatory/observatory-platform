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
from academic_observatory.reports.charts import output_types_time_chart
from academic_observatory.reports.charts.generic_time_chart import *


class CitationCountTimeChart(GenericTimeChart):
    """Generates a plot of Citation Counts per Published Year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020),
                 chart_type: str = 'count'):

        self.chart_type = chart_type
        columns = [
            'Citation Count',
            'Cited Articles',
            'Citations to OA Outputs',
            'Citations to Gold Outputs',
            'Citations to Green Outputs',
            'Citations to Hybrid Outputs'
        ]
        super().__init__(df, columns, identifier, year_range)

    def process_data(self):
        if 'total_oa' not in self.df.columns:
            self.df['total_oa'] = self.df.oa
        if self.chart_type in ['per-article', 'advantage']:
            self.df['Non-OA'] = (self.df.total_citations - self.df.oa_citations
                                 ) / (self.df.total - self.df.total_oa)
            self.df['All Outputs'] = self.df.total_citations / self.df.total
            self.df['Open Access'] = self.df.oa_citations / self.df.total_oa
            self.df['Gold OA'] = self.df.gold_citations / self.df.gold
            self.df['Green OA'] = self.df.green_citations / self.df.green
            self.df['Hybrid OA'] = self.df.hybrid_citations / self.df.hybrid

            if self.chart_type == 'advantage':
                self.df['Non-Open Access'] = self.df['Non-OA'] /   \
                    self.df['All Outputs']
                self.df['Open Access'] = self.df['Open Access'] /  \
                    self.df['All Outputs']
                self.df['Gold OA'] = self.df['Gold OA'] /          \
                    self.df['All Outputs']
                self.df['Green OA'] = self.df['Green OA'] /        \
                    self.df['All Outputs']
                self.df['Hybrid OA'] = self.df['Hybrid OA'] /      \
                    self.df['All Outputs']
                self.columns = ['Non-Open Access',
                                'Open Access',
                                'Gold OA',
                                'Green OA',
                                'Hybrid OA']
                self.melt_var_name = 'Citation Advantage'

            elif self.chart_type == 'per-article':
                self.columns = ['All Outputs',
                                'Open Access',
                                'Gold OA',
                                'Green OA',
                                'Hybrid OA']
                self.melt_var_name = 'Citations per Output'
        self.figdata = super().process_data()
        return self.figdata

    def plot(self, palette=None, ax=None, **kwargs):
        if not palette:
            if self.chart_type == 'count':
                palette = ['blue', 'orange',
                           'black', 'gold', 'darkgreen', 'orange']
            if self.chart_type in ['per-article', 'advantage']:
                palette = ['red', 'black', 'gold', 'darkgreen', 'orange']
        if self.chart_type == 'advantage':
            lines = {'y': 1,
                     'xmin': 0,
                     'xmax': 0,
                     'color': 'grey',
                     'linestyle': 'dashed'}
        else:
            lines = None

        self.fig = super().plot(palette, ax=ax, lines=lines, **kwargs)
        return self.fig