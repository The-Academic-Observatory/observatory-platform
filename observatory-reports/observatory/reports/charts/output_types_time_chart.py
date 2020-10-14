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

from observatory.reports import defaults
from .generic_time_chart import GenericTimeChart


class OutputTypesTimeChart(GenericTimeChart):
    """Generate a Plot of Output Types Over Time

    Shares the `types_palette` with OutputTypesPieChart
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020),
                 typecolumn: str = 'type',
                 countcolumn: str = 'count'
                 ):
        """Initialisation function
        """

        columns = ['Output Types', 'value']
        self.typecolumn = typecolumn
        self.countcolumn = countcolumn
        super().__init__(df, columns, identifier, year_range)
        self.melt_var_name = 'Output Types'

    def process_data(self):
        """Data selection and processing function
        """

        self.df['Output Types'] = self.df[self.typecolumn]
        self.df['value'] = self.df[self.countcolumn]
        columns = ['id', 'Year of Publication'] + self.columns
        self.columns = defaults.output_types
        figdata = self.df[self.df.id == self.identifier][columns]
        figdata.sort_values('Year of Publication', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self,
             palette=None,
             ax=None,
             **kwargs):
        """Plotting function
        """

        if not palette:
            palette = defaults.outputs_palette
        super().plot(palette=palette,
                     ax=ax, **kwargs)
        plt.ylabel('Number of Outputs')
        return self.fig
