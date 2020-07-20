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

from academic_observatory.reports import AbstractObservatoryChart
from academic_observatory.reports import chart_utils
from academic_observatory.reports.chart_utils import _collect_kwargs_for


class TimePlotLayout(AbstractObservatoryChart):
    """Layout made up of TimePlots
    """

    def __init__(self, df,
                 plots,
                 **kwargs):
        """Initialisation function

        param: df: pd.DataFrame in COKI standard format
        param: plots: a list of dicts, each of which must
                       conform to the following structure:

{
    year_range: (2010, 2018),
            # A tuple with two elements containing a start and end year
    y_column: 'Total Gold OA (%)
            # A str containing a column name with y values
    unis: ['id1', 'id2', 'id3']
            # An ordered list of identifiers for plotting
}
        """

        self.df = df
        assert type(plots) == list
        for plot in [p for p in plots]:
            assert type(plot) == dict
            for k in ['year_range', 'y_column', 'unis']:
                assert k in plot
        self.plots = plots
        self.kwargs = kwargs
        super().__init__(df)

    def process_data(self, **kwargs):
        """Data selection and processing function
        """

        self.plot_data = [None for _ in range(len(self.plots))]
        for i, plot in enumerate(self.plots):
            year_range = plot.get('year_range')
            years = range(*year_range)
            self.plot_data[i] = self.df[
                self.df.published_year.isin(years) &
                self.df.id.isin(plot.get('unis'))
                ].sort_values('published_year')

    def plot(self, fig=None,
             ylabel_adjustment=0.025,
             panel_labels=False,
             panellable_adjustment=0.01,
             **kwargs):
        """Plotting function
        """

        figure_kwargs = {k: kwargs[k] for k in kwargs.keys() &
                         {'figsize', 'sharey', 'sharex'}}

        gridspec_kwargs = {k: kwargs[k] for k in kwargs.keys() &
                           {'wspace', 'hspace'}}
        if not fig:
            fig = plt.figure(**figure_kwargs)
        layout = fig.add_gridspec(1, len(self.plots), **gridspec_kwargs)

        for i, plot in enumerate(self.plots):
            subspec = layout[i].subgridspec(len(plot.get('unis')), 1)
            for j, uni in enumerate(plot.get('unis')):
                ax = fig.add_subplot(subspec[j])
                ax_df = self.plot_data[i]
                ax_data = ax_df[ax_df.id == uni]
                ax_data.plot(x='published_year', y=plot.get('y_column'),
                             ax=ax, legend=False, title=chart_utils.id2name(self.df, uni))
                if plot.get('markerline'):
                    if ax.is_first_row():
                        ax.axvline(plot.get('markerline'),
                                   0, 1, color='grey',
                                   linestyle='dashed', clip_on=False)
                    else:
                        ax.axvline(plot.get('markerline'),
                                   0, 1.2, color='grey',
                                   linestyle='dashed', clip_on=False)
                ax.set(**_collect_kwargs_for(ax.set, plot))

        all_axes = fig.get_axes()

        for ax in all_axes:
            for sp in ax.spines.values():
                sp.set_visible(False)

            ax.get_xaxis().set_visible(False)
            ax.spines['left'].set_visible(True)

            ax.title.set_ha('left')
            ax.title.set_position([0.03, 0.95])
            if ax.is_last_row():
                ax.spines['bottom'].set_visible(True)
                ax.get_xaxis().set_visible(True)

        subplots_params = []
        for i in range(len(self.plots)):
            subplots_params.append(layout[i].get_position(fig))

        for i, plot in enumerate(self.plots):
            ylabel = plot.get('y_column')
            xpos = subplots_params[i].x0
            ypos = subplots_params[i].y1
            fig.text(xpos - ylabel_adjustment, 0.5, ylabel,
                     ha='center', va='center', rotation='vertical')
            if panel_labels:
                labels = ['A', 'B', 'C', 'D', 'E', 'F']
                fig.text(xpos, ypos + panellable_adjustment,
                         labels[i], fontsize='xx-large', fontweight='bold')

        return fig
