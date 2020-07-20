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

from observatory_platform.reports.charts.output_types_time_chart import *


class OApcTimeChart(GenericTimeChart):
    """Generate a Plot of Standard OA Types Over Time

    Produces a standard line plot coloured by OA type for
    a single id
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020),
                 ):
        """Initialisation function
        """

        columns = defaults.oa_types
        super().__init__(df, columns, identifier, year_range)
        self.melt_var_name = 'Access Type'

    def plot(self, palette=defaults.oatypes_palette,
             ax=None, **kwargs):
        """Plotting function
        """

        self.fig = super().plot(palette, ax=ax, **kwargs)
        return self.fig
