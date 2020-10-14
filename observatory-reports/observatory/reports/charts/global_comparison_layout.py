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

from observatory.reports.charts.scatter_plot import *


class GlobalComparisonLayout(AbstractObservatoryChart):
    """Layout containing global and regional comparison scatter plot
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int):
        """Initialisation function
        """

        self.df = df
        self.identifier = identifier
        self.focus_year = focus_year
        self.globalscatter = ScatterPlot(df,
                                         x='Total Green OA (%)',
                                         y='Total Gold OA (%)',
                                         filter_name='published_year',
                                         filter_value=focus_year,
                                         hue_column='region',
                                         focus_id=self.identifier)

        self.region_name = self.df[
            self.df.id == self.identifier].region.unique()[0]
        self.region_data = self.df[self.df.region == self.region_name]
        self.regionscatter = ScatterPlot(self.region_data,
                                         x='Total Green OA (%)',
                                         y='Total Gold OA (%)',
                                         filter_name='published_year',
                                         filter_value=focus_year,
                                         hue_column='country',
                                         focus_id=self.identifier)

    def process_data(self):
        """Data selection and processing function
        """

        self.globalscatter.process_data()
        self.regionscatter.process_data()

    def plot(self):
        """Plotting function
        """

        self.fig, axes = plt.subplots(nrows=1,
                                      ncols=2,
                                      figsize=(8, 3))
        self.globalscatter.plot(ax=axes[0], xlim=(0, 100), ylim=(0, 100))

        numcountries = len(self.region_data.country.unique())
        colorpalette = sns.color_palette(
            palette='bright', n_colors=numcountries)
        self.regionscatter.plot(ax=axes[1],
                                colorpalette=colorpalette)
        plt.subplots_adjust(wspace=1)
        return self.fig
