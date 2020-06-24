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

# Author: Cameron Neylon & Richard Hosking

import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc, artist
from IPython.display import HTML
from abc import ABC, abstractmethod

from academic_observatory.reports.chart_utils import _collect_kwargs_for, id2name
from academic_observatory.reports import defaults


class AbstractObservatoryChart(ABC):
    """Abstract Base Class for Charts

    Mainly provided for future development to provide any general
    methods or objects required for Observatory Charts.

    All chart methods are required to implement a `process_data` and
    `plot` method. The first conducts any filtering or reshaping of
    the data and the second plots with defaults. In general
    the init method should place the relevant data as a pd.DataFrame
    in self.df and the `process_data` method should modify this in
    place.

    The plot method should accept an ax argument which defaults to
    None. The default behaviour should be to create and return a
    matplotlib figure. Arguments for modifying either the ax or
    the figure should be accepted and passed to the correct object,
    generally after construction.
    """

    def __init__(self, df, **kwargs):
        self.df = df

    def _check_df(self):
        #
        # TODO Some error checking on df being in right form
        return

    @abstractmethod
    def process_data(self):
        """Abstract Data Processing Method

        All chart classes should implement a process_data method
        which does any necessary reshaping or filtering of data
        and modifies self.df in place to provide the necessary
        data in the right format
        """
        pass

    @abstractmethod
    def plot(self, ax=None, fig=None, **kwargs):
        """Abstract Plot Method

        All chart classes should have a plot method which draws the
        figure and returns it.
        """
        pass

    def watermark(self,
                  image_file: str,
                  xpad: int = 0,
                  position: str = 'lower right') -> matplotlib.figure:
        """Modifies self.fig to add a watermark image to the graph

        Will throw an error if self.fig does not exist, ie if the plot
        method has not yet been called.

        :param image_file: str containing path to the watermark image file
        :type image_file: str
        :param xpad: Padding in pixels to move the watermark. This is required
        because the precise limits of the figure can't be easily determined,
        defaults to 0
        :type xpad: int, optional
        :param position: str describing the position to place the watermark
        image, defaults to 'lower right'
        :type position: str, optional
        :return: A matplotlib figure instance with the watermark added
        :rtype: matplotlib.figure
        """

        self.fig.set_dpi(300)

        wm_data = matplotlib.image.imread(image_file)
        wm_size_px = wm_data.shape
        # figbounds = self.fig.get_tightbbox(
        #     self.fig.canvas.get_renderer(),
        #     bbox_extra_artists=self.fig.get_children()).corners()

        figsize = self.fig.get_size_inches()
        dpi = self.fig.get_dpi()

        x_displacement = 20
        y_displacement = x_displacement

        x_pos = x_displacement
        y_pos = y_displacement
        if position.endswith('right'):
            x_pos = (figsize[0] * dpi) - x_displacement - wm_size_px[1] - xpad

        if position.startswith('upper'):
            y_pos = (figsize[1] * dpi) - 10 - wm_size_px[0]

        self.fig.figimage(wm_data, x_pos, y_pos, alpha=0.2, origin='upper')
        return self.fig